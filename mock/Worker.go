package main

import (
	"github.com/aws/aws-lambda-go/events"
)

type Worker interface {
	Work(*events.SQSMessage) Result
}
