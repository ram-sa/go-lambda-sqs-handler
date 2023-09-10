package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type BatchHandler struct {
	BackOffSettings     BackOffSettings
	Context             context.Context
	DiscardUnhandleable bool
	FailureDlqUrl       string
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

type HandlingError struct {
	MessageId string
	Status    Status
	Error     error
}

func (b *BatchHandler) HandleEvent(event *events.SQSEvent, worker Worker) (WorkReport, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	results := make(map[Status][]Result)

	// TODO pass context to stop execution
	// TODO defer to failure on panic
	for _, msg := range event.Records {
		go func(msg *events.SQSMessage) {
			ch <- worker.Work(msg)
		}(&msg)
	}

	for i := 0; i < len; i++ {
		r := <-ch
		// TODO Call r.Validate before mapping value and wrap errors
		// 'https://go.dev/doc/go1.20#errors'
		results[r.Status] = append(results[r.Status], r)
	}

	errs := b.handleResults(results)
	return generateReport(event, results, errs)
}

// Process the worker's results and handles them accordingly.
func (b *BatchHandler) handleResults(results map[Status][]Result) []HandlingError {
	// If there are no Retries or Failures there's no reason to manually delete
	// messages from the queue, as we can just report a success to the lambda framework.
	if results[Retry] == nil && results[Failure] == nil {
		return nil
	}

	var errs []HandlingError

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
func (b *BatchHandler) handleRetries(results []Result) []HandlingError {
	var errs []HandlingError

	for _, r := range results {
		v, err := b.getNewVisibility(r.Message)

		if err == nil {
			err = b.changeVisibility(r.Message, v)
		}

		if err != nil {
			errs = append(errs, HandlingError{
				MessageId: r.Message.MessageId,
				Status:    r.Status,
				Error:     err,
			})
			if err = b.solveUnhandleable(r.Message); err != nil {
				errs = append(errs, HandlingError{
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
func (b *BatchHandler) handleFailures(results []Result) []HandlingError {
	var errs []HandlingError

	for _, r := range results {
		if b.FailureDlqUrl != "" {
			if err := b.sendMessage(r.Message, &b.FailureDlqUrl); err != nil {
				errs = append(errs, HandlingError{
					MessageId: r.Message.MessageId,
					Status:    r.Status,
					Error:     err,
				})
				if err = b.solveUnhandleable(r.Message); err != nil {
					errs = append(errs, HandlingError{
						MessageId: r.Message.MessageId,
						Status:    r.Status,
						Error:     err,
					})
				}
			}
		}

		if err := b.deleteMessage(r.Message); err != nil {
			errs = append(errs, HandlingError{
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
func (b *BatchHandler) handleSkipsOrSuccesses(results []Result) []HandlingError {
	var errs []HandlingError

	for _, r := range results {
		if err := b.deleteMessage(r.Message); err != nil {
			errs = append(errs, HandlingError{
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
