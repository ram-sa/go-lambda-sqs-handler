package handler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Interface to enable mocking of a SQSClient, when necessary
type SQSClient interface {
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type BatchHandler struct {
	BackOffSettings BackOffSettings
	Context         context.Context
	FailureDlqURL   string
	SQSClient       SQSClient
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

func NewBatchHandler(c context.Context, failureDlqURL string, sqsClient SQSClient) *BatchHandler {
	return &BatchHandler{
		BackOffSettings: NewBackOffSettings(),
		Context:         c,
		FailureDlqURL:   failureDlqURL,
		SQSClient: func(client SQSClient, c context.Context) SQSClient {
			if client != nil {
				return client
			}
			cfg, err := config.LoadDefaultConfig(c)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			return sqs.NewFromConfig(cfg)
		}(sqsClient, c),
	}
}

func (b *BatchHandler) HandleEvent(event *events.SQSEvent, worker Worker) (events.SQSEventResponse, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	results := make(map[Status][]Result)
	timeout := setTimeout(b.Context)

	for _, msg := range event.Records {
		go wrapWorker(b.Context, msg, worker, ch)
	}

out:
	for i := 0; i < len; i++ {
		select {
		case <-timeout:
			fmt.Println(errors.New("the lambda function timed out"))
			break out
		case r := <-ch:
			// Invalid status are handled as failures
			if err := r.validate(); err == nil {
				results[r.Status] = append(results[r.Status], r)
			} else {
				if r.Message != nil {
					r.Error = errors.Join(r.Error, fmt.Errorf("invalid status property `%v`", r.Status))
					r.Status = Failure
					results[r.Status] = append(results[r.Status], r)
				} else {
					fmt.Println(errors.New("worker did not return a valid message"))
				}
			}
		}
	}

	eResp, hErrs := b.handleResults(results)
	printReport(event, results, hErrs)

	// for debug
	fmt.Println(eResp)

	return eResp, nil
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
func wrapWorker(c context.Context, msg events.SQSMessage, worker Worker, ch chan<- Result) {
	defer func(ch chan<- Result) {
		if r := recover(); r != nil {
			err := fmt.Errorf("worker panic:\n%v", r)
			ch <- Result{
				Message: &msg,
				Status:  Failure,
				Error:   err,
			}
		}
	}(ch)
	ch <- worker.Work(c, msg)
}

// Process the worker's results and handles them accordingly, returning an SQSEventResponse
// containing any messages from the batch that need to be reprocessed.
func (b *BatchHandler) handleResults(results map[Status][]Result) (events.SQSEventResponse, []HandlerError) {
	// If there are no Retries or Failures there's no reason to iterate through the
	// results, as we can just report a success to the lambda framework.
	if results[Retry] == nil && results[Failure] == nil {
		return events.SQSEventResponse{}, nil
	}

	var hErrs []HandlerError

	if results[Failure] != nil {
		hErrs = append(hErrs, b.handleFailures(results[Failure])...)
	}

	var res events.SQSEventResponse

	if results[Retry] != nil {
		rHErrs, items := b.handleRetries(results[Retry])
		res.BatchItemFailures = items
		hErrs = append(hErrs, rHErrs...)
	}

	return res, hErrs //, err
}

// Handles transient errors by altering a message's VisibilityTimeout with an
// exponential backoff value.
func (b *BatchHandler) handleRetries(results []Result) ([]HandlerError, []events.SQSBatchItemFailure) {
	s := len(results)
	items := make([]events.SQSBatchItemFailure, s)
	var errs []HandlerError
	for i, r := range results {
		items[i] = events.SQSBatchItemFailure{ItemIdentifier: r.Message.MessageId}
		v, err := b.getNewVisibility(r.Message)

		if err == nil {
			err = b.changeVisibility(r.Message, v)
		}

		if err != nil {
			errs = append(errs, HandlerError{
				MessageId: r.Message.MessageId,
				Error:     err,
			})
		}
	}

	return errs, items
}

// Handles unrecoverable errors by removing said messages from the queue and sending
// them to a designated DLQ, if available.
func (b *BatchHandler) handleFailures(results []Result) []HandlerError {
	if b.FailureDlqURL == "" {
		return nil
	}

	var errs []HandlerError

	for _, r := range results {
		if err := b.sendMessage(r.Message, &b.FailureDlqURL); err != nil {
			errs = append(errs, HandlerError{
				MessageId: r.Message.MessageId,
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

// Builds a queue URL from its ARN
func generateQueueUrl(queueArn string) (*string, error) {
	s := strings.Split(queueArn, ":")
	if len(s) != 6 {
		return nil, fmt.Errorf("unable to parse queue's ARN: %v", queueArn)
	}
	url := fmt.Sprintf("https://sqs.%v.amazonaws.com/%v/%v", s[3], s[4], s[5])
	return &url, nil
}
