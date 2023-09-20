/*
Package sqshandler implements a
*/
package sqshandler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type BatchHandler struct {
	BackOff       BackOff
	Context       context.Context
	FailureDlqURL string
	SQSClient     SQSClient
}

// Interface to enable mocking of a SQSClient, usually for testing purposes
type SQSClient interface {
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type handlerError struct {
	MessageId string
	Error     error
}

/*
New creates an instance of BatchHandler with default values for
exponential backoff on retries, no DLQ for failed messages and a sqs.Client
with default configurations.
*/
func New(c context.Context) *BatchHandler {
	return &BatchHandler{
		BackOff:       NewBackOff(),
		Context:       c,
		FailureDlqURL: "",
		SQSClient: func(c context.Context) SQSClient {
			cfg, err := config.LoadDefaultConfig(c)
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			return sqs.NewFromConfig(cfg)
		}(c),
	}
}

// HandleEvent
func (b *BatchHandler) HandleEvent(event *events.SQSEvent, worker Worker) (events.SQSEventResponse, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	results := make(map[Status][]Result)
	timer, _ := setTimer(b.Context)

	for _, msg := range event.Records {
		go workWrapped(b.Context, msg, worker, ch)
	}

out:
	for i := 0; i < len; i++ {
		select {
		case <-timer.C:
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

	timer.Stop()
	eResp, hErrs := b.handleResults(results)
	printReport(event, results, hErrs)

	return eResp, nil
}

// Starts a timer that will run until the deadline for processing results
func setTimer(c context.Context) (*time.Timer, time.Time) {
	deadline, ok := c.Deadline()
	if !ok {
		// Defaults to 15 minutes (lambda max)
		deadline = time.Now().Add(15 * time.Minute)
	}
	// Reserves 5 seconds for processing results
	deadline = deadline.Add(-5 * time.Second)
	return time.NewTimer(time.Until(deadline)), deadline
}

// Wraps the custom worker function in order to recover from panics
func workWrapped(c context.Context, msg events.SQSMessage, worker Worker, ch chan<- Result) {
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
func (b *BatchHandler) handleResults(results map[Status][]Result) (events.SQSEventResponse, []handlerError) {
	// If there are no Retries or Failures there's no reason to iterate through the
	// results, as we can just report a success to the lambda framework.
	if results[Retry] == nil && results[Failure] == nil {
		return events.SQSEventResponse{}, nil
	}

	var hErrs []handlerError

	if results[Failure] != nil {
		hErrs = append(hErrs, b.handleFailures(results[Failure])...)
	}

	var res events.SQSEventResponse

	if results[Retry] != nil {
		items, errs := b.handleRetries(results[Retry])
		res.BatchItemFailures = items
		hErrs = append(hErrs, errs...)
	}

	return res, hErrs
}

// Handles transient errors by altering a message's VisibilityTimeout with an
// exponential backoff value.
func (b *BatchHandler) handleRetries(results []Result) ([]events.SQSBatchItemFailure, []handlerError) {
	s := len(results)
	items := make([]events.SQSBatchItemFailure, s)
	var errs []handlerError

	b.BackOff.validate()

	for i, r := range results {
		items[i] = events.SQSBatchItemFailure{ItemIdentifier: r.Message.MessageId}
		v, err := b.getNewVisibility(r.Message)

		if err == nil {
			err = b.changeVisibility(r.Message, v)
		}

		if err != nil {
			errs = append(errs, handlerError{
				MessageId: r.Message.MessageId,
				Error:     err,
			})
		}
	}

	return items, errs
}

// Handles unrecoverable errors by sending them to a designated DLQ, if available.
func (b *BatchHandler) handleFailures(results []Result) []handlerError {
	if b.FailureDlqURL == "" {
		return nil
	}

	var errs []handlerError

	for _, r := range results {
		if err := b.sendMessage(r.Message, &b.FailureDlqURL); err != nil {
			errs = append(errs, handlerError{
				MessageId: r.Message.MessageId,
				Error:     err,
			})
		}
	}

	return errs
}

// Retrieves a new visibility duration for the message.
func (b *BatchHandler) getNewVisibility(e *events.SQSMessage) (int32, error) {
	att, ok := e.Attributes["ApproximateReceiveCount"]
	d, err := strconv.Atoi(att)
	if !ok || err != nil || d < 1 {
		errParse := errors.New("unable to parse attribute `ApproximateReceiveCount`")
		return 0, errors.Join(errParse, err)
	}
	newVisibility := int32(b.BackOff.calculateBackOff(float64(d)))
	return newVisibility, nil
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
