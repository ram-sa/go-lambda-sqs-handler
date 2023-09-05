package main

// Go lambda functions run on an AL2 custom runtime. To upload it,
// deploy the package using:
// `GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o ./deploy/bootstrap main.go`
// Then, zip the bootstrap into funcionName.zip and upload it.
// Remember to correctly point the lambda's function handler to `main`

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type MyEvent struct {
	Name string `json:"name"`
}

func HandleRequest(ctx context.Context, sqsEvent events.SQSEvent) (string, error) {
	return fmt.Sprintf("%v", sqsEvent), nil
}

func main() {
	lambda.Start(HandleRequest)
}
