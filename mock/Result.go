package main

import (
	"github.com/aws/aws-lambda-go/events"
	"gopkg.in/go-playground/validator.v9"
)

type Result struct {
	Message *events.SQSMessage
	Status  Status `validate:"oneof=FAILURE RETRY SKIP SUCCESS"`
	Error   error
}

func (r *Result) Validate() error {
	validate := validator.New()
	return validate.Struct(r)
}
