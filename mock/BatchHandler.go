package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type BatchHandler struct {
	BackOffSettings     BackOffSettings
	Context             context.Context
	DiscardUnhandleable bool
	FailureDlqURL       string
	SQSClient           sqs.Client
}

type BackOffSettings struct {
	InitTimeoutSec, MaxTimeoutSec int32
	Multiplier, RandFactor        float64
}

const (
	DefaultIniTimeout = 5
	DefaultMaxTimeout = 300
	DefaultMultiplier = 2.5
	DefaultRandFactor = 0.3
)

type HandlerError struct {
	MessageId string
	Status    Status
	Error     error
}

func NewBackOffSettings() BackOffSettings {
	return BackOffSettings{
		InitTimeoutSec: DefaultIniTimeout,
		MaxTimeoutSec:  DefaultMaxTimeout,
		Multiplier:     DefaultMultiplier,
		RandFactor:     DefaultRandFactor,
	}
}

func NewBatchHandler(c context.Context, failureDlqURL string, discardUnhandleable bool) *BatchHandler {
	return &BatchHandler{
		BackOffSettings:     NewBackOffSettings(),
		Context:             c,
		DiscardUnhandleable: discardUnhandleable,
		FailureDlqURL:       failureDlqURL,
		// TODO SQSClient: ,
	}
}

func (b *BatchHandler) HandleEvent(event *events.SQSEvent, worker Worker) (WorkReport, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	results := make(map[Status][]Result)
	timeout := setTimeout(b.Context)

	for _, msg := range event.Records {
		go wrapWorker(b.Context, &msg, worker, ch)
	}

out:
	for i := 0; i < len; i++ {
		select {
		case <-timeout:
			fmt.Println(errors.New("the lambda function timed out"))
			break out
		default:
			r := <-ch
			// Invalid status are handled as failures
			if err := r.Validate(); err != nil {
				r.Error = errors.Join(r.Error, fmt.Errorf("invalid status property `%v`", r.Status))
				r.Status = Failure
			}
			results[r.Status] = append(results[r.Status], r)
		}
	}

	errs := b.handleResults(results)
	return generateReport(event, results, errs)
}

// Sets a deadline for processing results
func setTimeout(c context.Context) <-chan time.Time {
	deadline, ok := c.Deadline()
	if !ok {
		// Defaults to 15 minutes (lambda max)
		deadline = time.Now().Add(15 * time.Minute)
	}
	// Reserves 5 seconds for processing results
	deadline = deadline.Add(-5 * time.Second)

	return time.After(time.Until(deadline))
}

// Wraps the custom worker function in order to recover from panics
func wrapWorker(c context.Context, msg *events.SQSMessage, worker Worker, ch chan<- Result) {
	defer func(ch chan<- Result) {
		if r := recover(); r != nil {
			err := fmt.Errorf("worker panic:\n%v", r)
			ch <- Result{
				Message: msg,
				Status:  Failure,
				Error:   err,
			}
		}
	}(ch)

	ch <- worker.Work(c, msg)
}

// Process the worker's results and handles them accordingly.
func (b *BatchHandler) handleResults(results map[Status][]Result) []HandlerError {
	// If there are no Retries or Failures there's no reason to manually delete
	// messages from the queue, as we can just report a success to the lambda framework.
	if results[Retry] == nil && results[Failure] == nil {
		return nil
	}

	var errs []HandlerError

	if results[Retry] != nil {
		errs = append(errs, b.handleRetries(results[Retry])...)
	} else if results[Failure] != nil {
		errs = append(errs, b.handleFailures(results[Failure])...)
	}
	ss := append(results[Skip], results[Success]...)
	errs = append(errs, b.handleSkipsOrSuccesses(ss)...)

	return errs
}

// Handles transient errors by altering a message's VisibilityTimeout with an
// exponential backoff value.
func (b *BatchHandler) handleRetries(results []Result) []HandlerError {
	var errs []HandlerError

	for _, r := range results {
		v, err := b.getNewVisibility(r.Message)

		if err == nil {
			err = b.changeVisibility(r.Message, v)
		}

		if err != nil {
			errs = append(errs, HandlerError{
				MessageId: r.Message.MessageId,
				Status:    r.Status,
				Error:     err,
			})
			if err = b.solveUnhandleable(r.Message); err != nil {
				errs = append(errs, HandlerError{
					MessageId: r.Message.MessageId,
					Status:    r.Status,
					Error:     err,
				})
			}
		}
	}

	return errs
}

// Handles unrecoverable errors by removing said messages from the queue and sending
// them to a designated DLQ, if available.
func (b *BatchHandler) handleFailures(results []Result) []HandlerError {
	var errs []HandlerError

	for _, r := range results {
		if b.FailureDlqURL != "" {
			if err := b.sendMessage(r.Message, &b.FailureDlqURL); err != nil {
				errs = append(errs, HandlerError{
					MessageId: r.Message.MessageId,
					Status:    r.Status,
					Error:     err,
				})
				if err = b.solveUnhandleable(r.Message); err != nil {
					errs = append(errs, HandlerError{
						MessageId: r.Message.MessageId,
						Status:    r.Status,
						Error:     err,
					})
				}
			}
		}

		if err := b.deleteMessage(r.Message); err != nil {
			errs = append(errs, HandlerError{
				MessageId: r.Message.MessageId,
				Status:    r.Status,
				Error:     err,
			})
		}

		/**
		b.solveUnhandleable isn't called here since it eithers leave the message as is
		or attempts to delete it from the queue. If the previous delete failed, there's
		little reason to believe that attempting it again will magically solve the issue,
		as the AWS client already has built-in fallback functionality.
		**/
	}

	return errs
}

// Handles skipped or succesfully processed messages by removing them from the queue.
// Called when part of the batch encountered errors.
func (b *BatchHandler) handleSkipsOrSuccesses(results []Result) []HandlerError {
	var errs []HandlerError

	for _, r := range results {
		if err := b.deleteMessage(r.Message); err != nil {
			errs = append(errs, HandlerError{
				MessageId: r.Message.MessageId,
				Status:    r.Status,
				Error:     err,
			})
		}
	}

	return errs
}

// Creates a new visibility duration for the message.
func (b *BatchHandler) getNewVisibility(e *events.SQSMessage) (int32, error) {
	att, ok := e.Attributes["ApproximateReceiveCount"]
	d, err := strconv.Atoi(att)
	if !ok || err != nil {
		errParse := errors.New("unable to parse attribute `ApproximateReceiveCount`")
		return 0, errors.Join(errParse, err)
	}
	nVis := b.calculateBackoff(float64(d))
	return nVis, nil
}

// Calculates an exponential backoff based on the values configured
// in [BackoffSettings] and how many delivery attempts have occurred,
// then adds in jitter in the range set in [BackoffSettings.RandFactor].
// Based on `https://github.com/cenkalti/backoff/blob/v4/exponential.go#L149`
func (b *BatchHandler) calculateBackoff(deliveries float64) int32 {
	s := b.BackOffSettings
	bo := int32(float64(s.InitTimeoutSec) * s.Multiplier * deliveries)

	if s.RandFactor > 0 {
		d := s.RandFactor * float64(bo)
		min := float64(bo) - d
		max := float64(bo) + d
		bo = int32(min + (rand.Float64() * (max - min + 1)))
	}

	if s.MaxTimeoutSec < bo {
		return s.MaxTimeoutSec
	}
	return bo
}

// Requests the original SQS queue to change the message's
// visibility timeout to the provided value.
func (b *BatchHandler) changeVisibility(message *events.SQSMessage, newVisibility int32) error {
	url, err := generateQueueUrl(message.EventSourceARN)
	if err == nil {
		_, err = b.SQSClient.ChangeMessageVisibility(b.Context, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          url,
			ReceiptHandle:     &message.ReceiptHandle,
			VisibilityTimeout: newVisibility,
		})
	}

	return err
}

// Removes a message from the queue
func (b *BatchHandler) deleteMessage(message *events.SQSMessage) error {
	url, err := generateQueueUrl(message.EventSourceARN)
	if err == nil {
		_, err = b.SQSClient.DeleteMessage(b.Context, &sqs.DeleteMessageInput{
			QueueUrl:      url,
			ReceiptHandle: &message.ReceiptHandle,
		})
	}

	return err
}

// Forwards a message to the designated queue
func (b *BatchHandler) sendMessage(message *events.SQSMessage, url *string) error {
	_, err := b.SQSClient.SendMessage(b.Context, &sqs.SendMessageInput{
		MessageBody: &message.Body,
		MessageAttributes: func(m map[string]events.SQSMessageAttribute) map[string]types.MessageAttributeValue {
			convAtt := make(map[string]types.MessageAttributeValue, len(m))
			for k, v := range m {
				convAtt[k] = types.MessageAttributeValue{
					DataType:         &v.DataType,
					BinaryValue:      v.BinaryValue,
					BinaryListValues: v.BinaryListValues,
					StringValue:      v.StringValue,
					StringListValues: v.StringListValues,
				}
			}
			return convAtt
		}(message.MessageAttributes),
		QueueUrl: url,
	})
	return err
}

// Removes the message from the queue if [BatchHandler.DiscardUnhandleable]
// is set.
func (b *BatchHandler) solveUnhandleable(message *events.SQSMessage) error {
	if !b.DiscardUnhandleable {
		return nil
	}
	return b.deleteMessage(message)
}

// Builds a queue URL from its ARN
func generateQueueUrl(queueArn string) (*string, error) {
	s := strings.Split(queueArn, ":")
	if len(s) != 6 {
		return nil, fmt.Errorf("unable to parse queue's ARN: %v", queueArn)
	}
	url := fmt.Sprintf("https://sqs.%v.amazonaws.com/%v/%v", s[3], s[4], s[5])
	return &url, nil
}
