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

type Handler struct {
	BackOff       BackOff
	Context       context.Context
	FailureDlqURL string
	SQSClient     SQSClient
}

// Interface to enable mocking of a SQSClient, usually for testing purposes
type SQSClient interface {
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Struct used to record errors that may occur while handling messages.
type handlerError struct {
	MessageId string
	Error     error
}

/*
New creates an instance of BatchHandler with default values for
exponential backoff on retries, no DLQ for failed messages and a sqs.Client
with default configurations.
*/
func New(c context.Context) *Handler {
	return &Handler{
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
func (b *Handler) HandleEvent(event *events.SQSEvent, worker Worker) (events.SQSEventResponse, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	results := make(map[Status][]Result)
	timer, _ := setTimer(b.Context)

	for _, msg := range event.Records {
		go workWrapped(b.Context, msg, worker, ch)
	}

	var err error
out:
	for i := 0; i < len; i++ {
		select {
		case <-timer.C:
			err = errors.New("the lambda function timed out")
			break out
		case r := <-ch:
			// Invalid status are handled as failures
			if vErr := r.validate(); vErr == nil {
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

	// Check if non-retry messages need to be manually deleted from the queue
	eResp, hErrs := b.handleResults(results, err != nil)

	printReport(event, results, hErrs)

	return eResp, err
}

// Starts a timer that will run until 5 seconds before the lambda
// timeout. These five seconds are reserved for processing results of
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
// containing any messages from the batch that need to be reprocessed. If cleanUp is set,
// manually deletes any messages where Status != Retry instead of relying on the returned
// events.SQSEventResponse.
func (b *Handler) handleResults(results map[Status][]Result, cleanUp bool) (events.SQSEventResponse, []handlerError) {
	var res events.SQSEventResponse
	var errs []handlerError

	if results[Failure] != nil {
		errs = append(errs, b.handleFailures(results[Failure])...)
	}

	if results[Retry] != nil {
		items, rErrs := b.handleRetries(results[Retry])
		res.BatchItemFailures = items
		errs = append(errs, rErrs...)
	}

	if cleanUp {
		errs = append(errs, b.handleCleanUp(results[Success], results[Skip], results[Failure])...)
	}

	return res, errs
}

// Handles transient errors by altering a message's VisibilityTimeout with an
// exponential backoff value.
func (b *Handler) handleRetries(results []Result) ([]events.SQSBatchItemFailure, []handlerError) {
	s := len(results)
	items := make([]events.SQSBatchItemFailure, s)
	var errs []handlerError

	b.BackOff.validate()

	for i, r := range results {
		items[i] = events.SQSBatchItemFailure{ItemIdentifier: r.Message.MessageId}
		vis, err := b.getNewVisibility(r.Message)

		if err == nil {
			var url *string
			url, err = generateQueueUrl(r.Message.EventSourceARN)
			if err == nil {
				err = b.changeVisibility(r.Message, vis, url)
			}
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
func (b *Handler) handleFailures(results []Result) []handlerError {
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

// Manually deletes any non-retry message from the queue.
func (b *Handler) handleCleanUp(results ...[]Result) []handlerError {
	var errs []handlerError

	for _, s := range results {
		for _, r := range s {
			url, err := generateQueueUrl(r.Message.EventSourceARN)
			if err == nil {
				err = b.deleteMessage(r.Message, url)
			}

			if err != nil {
				errs = append(errs, handlerError{
					MessageId: r.Message.MessageId,
					Error:     err,
				})
			}
		}
	}

	return errs
}

// Retrieves a new visibility duration for the message.
func (b *Handler) getNewVisibility(e *events.SQSMessage) (int32, error) {
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
func (b *Handler) changeVisibility(message *events.SQSMessage, newVisibility int32, url *string) error {
	_, err := b.SQSClient.ChangeMessageVisibility(b.Context, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          url,
		ReceiptHandle:     &message.ReceiptHandle,
		VisibilityTimeout: newVisibility,
	})

	return err
}

// Forwards a message to the designated queue
func (b *Handler) sendMessage(message *events.SQSMessage, url *string) error {
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

// Deletes a message from the queue
func (b *Handler) deleteMessage(message *events.SQSMessage, url *string) error {
	_, err := b.SQSClient.DeleteMessage(b.Context, &sqs.DeleteMessageInput{
		ReceiptHandle: &message.ReceiptHandle,
		QueueUrl:      url,
	})
	return err
}

// Builds a queue's URL from its ARN
func generateQueueUrl(queueArn string) (*string, error) {
	s := strings.Split(queueArn, ":")
	if len(s) != 6 {
		return nil, fmt.Errorf("unable to parse queue's ARN: %v", queueArn)
	}
	url := fmt.Sprintf("https://sqs.%v.amazonaws.com/%v/%v", s[3], s[4], s[5])
	return &url, nil
}
