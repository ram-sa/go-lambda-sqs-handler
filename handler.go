/*
Package sqshandler implements a lightweight wrapper for handling SQS Events inside
Lambda functions, reducing the complexity of dealing with different and sometimes
unexpected conditions to a set of defined results in a work-based abstraction.
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

/*
Handler functions as a configurable wrapper of an SQS Event, letting you control
some of the behaviors related to a message's lifetime inside the queue.

# BackOff

[BackOff] configures the exponential backoff values of
retried messages.

# Context

The Lambda's [context.Context]. Note that if this context has an invalid Deadline,
such as in a [context.TODO], Lambda's default of 15 minutes will be considered by the
Handler as the ceiling for execution.

# FailureDlqURL

This property is optional.

The URL of a DLQ to which messages with a [Status] of FAILURE will be sent to. This can be
the original queue's own DLQ, or a separate one. Make sure that your Lambda has an [execution role]
with enough permissions to write to said queue.

# SQSClient

A properly configured [sqs.Client], which will be used by the Handler to interface with
the queue.

# Configuring parallelism and number of retries

By design, a Handler will process all messages in a batch in parallel. Thus, the
degree of parallelism can be controlled by altering the property [Batch Size]
of your Lambda's trigger.

In much the same way, the amount of retries is tied to your queue's [Max Receive Count]
property, which defines how many delivery attempts will be made on a single message until
it is moved to a DLQ. For more information, see:

[execution role]: https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html
[Batch Size]: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#events-sqs-scaling
[Max Receive Count]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
*/
type Handler struct {
	BackOff       BackOff
	Context       context.Context
	FailureDlqURL string
	SQSClient     SQSClient
}

// Interface to enable mocking of a SQSClient, usually for testing purposes.
type SQSClient interface {
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Struct used to aggregate the processing state of a message, as returned by a Worker.
type result struct {
	message *events.SQSMessage
	status  Status `validate:"oneof=FAILURE RETRY SKIP SUCCESS"`
	err     error
}

// Struct used to record errors that may occur while handling messages.
type handlerError struct {
	MessageId string
	Error     error
}

/*
New creates an instance of [Handler] with default [BackOff] values
on retries, no DLQ set for messages with a [Status] of FAILURE and a [sqs.Client]
with default configurations, as per the following:

	cfg, err := config.LoadDefaultConfig(c)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	return sqs.NewFromConfig(cfg)
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

/*
HandleEvent is, as the name implies, the method called for handling an
[events.SQSEvent]. It works by separating the event into processable
messages which will then be given to a user defined [Worker].
After all messages have been processed, a [Report] will be printed to
stdout detailing their [Status] and any errors that may have occurred.

# Handling Messages

[event.SQSMessage]s inside the [events.SQSEvent] will be delivered
as they are to the [Worker] for processing, in parallel. The Handler
will wait for a [Status] callback for all messages up until 5 seconds
before the Lambda's configured [timeout]. After that, any message which
is still being processed will be ignored and returned to the queue
with their default [VisibilityTimeout] value. If any [Worker] panics
during execution, the Handler will consider that it failed to process
the message, and assign it such [Status] itself.

At this point, all possible messages should be in one of the four
states defined by [Status]. Any property that deviates from those 4
will be treated as a [Status.FAILURE], regardless if the message
was successfully processed or not, with an appended error describing
the issue.

# Reporting to the Lambda framework

While the Lambda framework accepts quite a few [handler signatures],
this Handler requires the use of

	func (context.Context, TIn) (TOut, error)

as we will be reporting any messages that we want to retry in a
[events.SQSEventResponse] in order to avoid unnecessary
calls to the SQS API.

With this option, the framework behaves differently depending on which
kind of response is given after execution. We will be focusing on 3
specific kinds (the complete list can be found [here]):
 1. Empty [events.SQSEventResponse], no error
 2. Populated [events.SQSEventResponse], no error
 3. Anything, error

These 3 responses will be utilized in different scenarios, which
will vary depending on the [Status] reported by your [Worker]:

  - If all messages were reported as [Status.SUCCESS], [Status.SKIP]
    or [Status.FAILURE] response number 1 will be used for the Lambda,
    signaling the framework that all messages were processed successfully
    and can be deleted from the queue. Failed messages will be reported
    as such by the Handler and sent to a DLQ, if one was configured.
    This response was chosen for failures due to how the framework
    interprets errors, which will be explained later.
  - If any message was reported as [Status.RETRY], then response number
    2 will be used, where [events.SQSEventResponse] will contain the
    ids of all messages that need reprocessing.
  - If the Lambda reached a timeout, then response number 3 will be used,
    and the Handler will behave a bit differently, due to how the framework
    deals with errors.

Which brings us to the next point:

# Reporting Errors

If any error reaches the Lambda framework, it will consider the execution
as having completely failed and any and all messages that were delivered
will be returned to the queue with their default [VisibilityTimeout].

Thus, even if your [Worker] returns an error on a [Status.FAILURE], it
will not be reported directly to the Lambda framework, as doing so would
mean that all messages, including successfully processed ones, would be
returned to the queue. These errors will still show up during logging,
as they are printed to stdout in the [Report].

On a timeout, however, the Handler doesn't know how to proceed with the
messages that didn't report back. Thus, the best idea is to return them
to the queue as they were, where they will follow the redelivery pattern
until the problem is fixed or they are naturally sent to a DLQ. Note,
however, that in order to ensure that only these messages will be returned,
the Handler will have to MANUALLY DELETE every message with a [Status] of
SUCCESS, SKIP and FAILURE from the queue.

# Handler Errors

When dealing with messages, the Handler will perform a few checks and make
calls to the SQS API (like [SendMessage] for sending failures to a DLQ and
[ChangeMessageVisibility] for exponential backoff on retries). If any of
these fail, they will be reported at the end of execution. Statuses will
try to be preserved as much as possible. Failures will still be removed
from the queue in order to avoid queue-poisoning and Retries will still be
sent back, only with their [VisibilityTimeout] unchanged. Note that these
behaviors are open to debate, so feel free to reach out with your thoughts
on the matter.

[timeout]: https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html#configuration-timeout-console
[VisibilityTimeout]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
[handler signatures]: https://docs.aws.amazon.com/lambda/latest/dg/golang-handler.html
[here]: https://repost.aws/knowledge-center/lambda-sqs-report-batch-item-failures
[SendMessage]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
[ChangeMessageVisibility]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
*/
func (b *Handler) HandleEvent(event *events.SQSEvent, worker Worker) (events.SQSEventResponse, error) {
	len := len(event.Records)
	ch := make(chan result, len)
	results := make(map[Status][]result)
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
			results[r.status] = append(results[r.status], r)
		}
	}
	timer.Stop()

	// Check if non-retry messages need to be manually deleted from the queue
	eResp, hErrs := b.handleResults(results, err != nil)

	printReport(event, results, hErrs)

	return eResp, err
}

// Process the worker's results and handles them accordingly, returning a SQSEventResponse
// containing any messages from the batch that need to be reprocessed. If cleanUp is set,
// manually deletes any messages where Status != Retry instead of relying on the returned
// events.SQSEventResponse.
func (b *Handler) handleResults(results map[Status][]result, cleanUp bool) (events.SQSEventResponse, []handlerError) {
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
func (b *Handler) handleRetries(results []result) ([]events.SQSBatchItemFailure, []handlerError) {
	s := len(results)
	items := make([]events.SQSBatchItemFailure, s)
	var errs []handlerError

	b.BackOff.validate()

	for i, r := range results {
		items[i] = events.SQSBatchItemFailure{ItemIdentifier: r.message.MessageId}
		vis, err := b.getNewVisibility(r.message)

		if err == nil {
			var url *string
			url, err = generateQueueUrl(r.message.EventSourceARN)
			if err == nil {
				err = b.changeVisibility(r.message, vis, url)
			}
		}

		if err != nil {
			errs = append(errs, handlerError{
				MessageId: r.message.MessageId,
				Error:     err,
			})
		}
	}

	return items, errs
}

// Handles unrecoverable errors by sending them to a designated DLQ, if available.
func (b *Handler) handleFailures(results []result) []handlerError {
	if b.FailureDlqURL == "" {
		return nil
	}

	var errs []handlerError

	for _, r := range results {
		if err := b.sendMessage(r.message, &b.FailureDlqURL); err != nil {
			errs = append(errs, handlerError{
				MessageId: r.message.MessageId,
				Error:     err,
			})
		}
	}

	return errs
}

// Manually deletes any non-retry message from the queue.
func (b *Handler) handleCleanUp(results ...[]result) []handlerError {
	var errs []handlerError

	for _, s := range results {
		for _, r := range s {
			url, err := generateQueueUrl(r.message.EventSourceARN)
			if err == nil {
				err = b.deleteMessage(r.message, url)
			}

			if err != nil {
				errs = append(errs, handlerError{
					MessageId: r.message.MessageId,
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
func workWrapped(c context.Context, msg events.SQSMessage, worker Worker, ch chan<- result) {
	defer func(ch chan<- result) {
		if r := recover(); r != nil {
			err := fmt.Errorf("worker panic:\n%v", r)
			ch <- result{&msg, Failure, err}
		}
	}(ch)

	s, e := worker.Work(c, msg)

	// Invalid status are handled as failures
	if !s.isValid() {
		e = errors.Join(e, fmt.Errorf("invalid status property `%v`", s))
		s = Failure
	}

	ch <- result{&msg, s, e}
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
