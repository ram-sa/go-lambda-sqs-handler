[![Go Reference](https://pkg.go.dev/badge/github.com/ram-sa/go-lambda-sqs-handler#Handler.svg)](https://pkg.go.dev/github.com/ram-sa/go-lambda-sqs-handler#Handler)
[![Go Report Card](https://goreportcard.com/badge/github.com/ram-sa/go-lambda-sqs-handler)](https://goreportcard.com/report/github.com/ram-sa/go-lambda-sqs-handler)

# go-lambda-sqs-handler
A lightweight wrapper for handling SQS Events inside 
Lambda functions, in Go.

## Description
Dealing with single events inside Lambda functions is usually pretty straightforward, as the event is either handled succesfully, or not.

Dealing with multiple messages inside a single event, however, usually proves harder. What if only one message couldn't be handled? **Should I return an error?** What if many of them couldn't, but not all? **Do I delete them manually from the queue?** If I set up a DLQ, does that mean that all messages that I couldn't handle will keep returning to the queue again and again until they finally go away? **What if I only want some of them to go away immediately, while others are retried?**

To alleviate said issues that many developers face when building Lambda functions with SQS triggers, **go-lambda-sqs-handler** aims to reduced the complexity of dealing with problems related to **infrastructure** so you can focus on **the logic that matters**.

## Features
### Simplified handling of different scenarios using a work-based abstraction.
This package works by wrapping an `sqs.Event` and shifiting focus to individual messages instead, which will be handled by your customized `Worker`. The `Result` of said `Worker.Work()` will then be processed individually for each message, which brings us to the second point:

### Extended error handling to accommodate both transient and non-transient scenarios.
By defining different `Status` properties, we can differentiate between which messages should be retried and which should not. 

Server returned a `503 Unavailable`? Return this message to the queue with an exponential `Backoff`.

Malformed message payload? Send it straight to a pre-configured `DLQ` to avoid queue-poisoning.

### Improved usage of Lambda execution times by handling multiple messages in parallel.
Make use of idle threads during I/O operations by handling other messages in the batch.

## Requirements
Make sure you have:
* A DLQ set on your SQS queue
* A Lambda function connected to a SQS trigger
* The Property `Report batch item failures` set to `Yes` on said trigger

## Instalation
Run `go get github.com/ram-sa/go-lambda-sqs-handler`

Then add `import "github.com/ram-sa/go-lambda-sqs-handler"` to your function handler

## Example
```go
package main
import "github.com/ram-sa/go-lambda-sqs-handler"

type MyWorker struct {
    // Set up your worker as needed
}

// This is the function that will be called by the Handler for processing a message.
func (w MyWorker) Work(c context.Context, m events.SQSMessage) {
    body := parseTheMessageBody(m.Body)
    doSomeStuff(body)
    err := sendItToSomeone(body)
    // Did everything go well?
    if isTransient(err){
        // Message will return to the queue with an exponential backoff.
        return sqshandler.Result{
            Message: &m, Status: Retry, Error: err
        }
    }else {
        // Everything went great, message acknowledged!
        return sqshandler.Result{
            Message: &m, Status: Success
        }
    }
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
    //Create a new handler with default configuration
	handler := sqshandler.New(ctx)
    //Initialize your worker as needed
    worker := MyWorker{}
	return handler.HandleEvent(&sqsEvent, worker)
}
```


For more information, check the docs at  https://pkg.go.dev/github.com/ram-sa/go-lambda-sqs-handler
