package sqshandler

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

/*
The Worker interface defines the main method which will be called by the [Handler]
for message processing: `Work`, and should be considered the entry point of your lambda
function.

# Work

This method will be called once per message by the [Handler], which will pass along the
Lambda context and the SQSMessage to be worked upon.

Note that the Lambda context contains its preconfigured timeout, which your Worker should
respect. Also note that if a timeout is imminent, the [Handler] will reserve 5 seconds of total runtime in order to
cleanup all previously handled messages. Any Work that does not return before that will be discarded, and its message
will be returned to the queue with its DefaultVisibilityTimeout.

After processing a message, the Worker should return a [Result] struct containing the relevant [Status].

If a Worker panics during execution, the [Handler] will consider that processing has failed,
and treat the message as if a FAILURE [Status] was returned.
*/
type Worker interface {
	Work(context.Context, events.SQSMessage) Result
}
