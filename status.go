package sqshandler

/*
Status defines the four possible states a Worker can report to
the [Handler]. These states are:

# SUCCESS

Denotes that the message has been processed successfully and thus
can be safely removed from the queue.

# SKIP

Denotes that the message is not relevant and has thus been skipped.
Functionally identical to SUCCESS, used for reporting purposes.

# RETRY

Denotes that the message was not processed successfully due to a
transient error. This signals the [Handler] to return the message
to the queue with an updated [VisibilityTimeout], following its
[Backoff] configuration.

# FAILURE

Denotes that the message was not processed successfully due to
immutable reasons that cannot be solved by simply retrying the
operation. This signals the [Handler] to send this message to
a DLQ, if one was specified during configuration, and then to
remove said message from the queue.

[VisibilityTimeout]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
*/
type Status string

const (
	Failure Status = "FAILURE"
	Retry   Status = "RETRY"
	Skip    Status = "SKIP"
	Success Status = "SUCCESS"
)

func (s *Status) isValid() bool {
	return (*s == Failure || *s == Retry || *s == Skip || *s == Success)
}
