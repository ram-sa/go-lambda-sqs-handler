package sqshandler

import (
	"github.com/aws/aws-lambda-go/events"
	"gopkg.in/go-playground/validator.v9"
)
/*
Status defines the four possible states a Worker can report to 
the [Handler]. These states are:

# SUCCESS

Denotes that the message has been processed succesfully and thus
can be safely removed from the queue.

# SKIP

Denotes that the message is not relevant and has thus been skipped. 
Functionally identical to SUCCESS, used for reporting purposes.

# RETRY

Denotes that the message was not processed succesfully due to a
transient error. This signals the [Handler] to return the message
to the queue with an updated `VisibilityTimeout`, following its 
[Backoff] configuration.

For information about a message's VisibilityTimeout, see:
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html

# FAILURE

Denotes that the message was not processed succesfully due to 
immutable reasons that cannot be solved by simply retrying the
operation. This signals the [Handler] to send this message to 
a DLQ, if one was specified during configuration, and then to 
remove said message from the queue. 
*/
type Status string

const (
	Failure Status = "FAILURE"
	Retry   Status = "RETRY"
	Skip    Status = "SKIP"
	Success Status = "SUCCESS"
)

/*
Result defines, as the name implies, the resulting state of the processing of a message, as returned by a [Worker].

# Message

The message that was processed by the [Worker].

# Status

The resulting status of the work, as defined in [Status]

# Error

Any relevant errors that arise during the processing of a message.
*/
type Result struct {
	Message *events.SQSMessage `validate:"required"`
	Status  Status             `validate:"oneof=FAILURE RETRY SKIP SUCCESS"`
	Error   error
}

// Validates if the struct contains an SQSMessage instance and that its Status property
// is either SUCCESS, SKIP, RETRY or FAILURE
func (r *Result) validate() error {
	validate := validator.New()
	return validate.Struct(r)
}
