package handler

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

type Worker interface {
	Work(context.Context, events.SQSMessage) Result
}
