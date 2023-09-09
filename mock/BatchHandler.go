package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type BatchHandler struct {
	BackOffSettings BackOffSettings
	Context         context.Context
	SQSClient       sqs.Client
}

type BackOffSettings struct {
	InitTimeoutSec int32
	MaxTimeoutSec  int32
	Multiplier     float64
	RandFactor     float64
}

const (
	DefaultIniTimeout = 5
	DefaultMaxTimeout = 300
	DefaultMultiplier = 2.5
	DefaultRandFactor = 0.3
)

var InvalidReceiveCountError = errors.New("Unable to parse attribute `ApproximateReceiveCount`.")

func (b *BatchHandler) HandleEvent(event *events.SQSEvent, worker Worker) (map[Status][]Result, error) {
	len := len(event.Records)
	ch := make(chan Result, len)
	m := make(map[Status][]Result)

	// TODO use context to stop execution
	for _, msg := range event.Records {
		go func(msg *events.SQSMessage) {
			ch <- worker.Work(msg)
		}(&msg)
	}

	for i := 0; i < len; i++ {
		r := <-ch
		// TODO Call r.Validate before mapping value and wrap errors
		// 'https://go.dev/doc/go1.20#errors'
		m[r.Status] = append(m[r.Status], r)
	}
	// TODO handle results properly
	b.handleResults(m)
	return m, nil
}

func (b *BatchHandler) handleResults(m map[Status][]Result) {
	//err := b.handleRetries(m[Retry])

}

//func (b *BatchHandler) handleRetries(results []Result) error {
//	if len(results) == 0 {
//		return nil
//	}
//
//	for _, r := range results {
//		v, eVis := b.getNewVisibility(r.Message)
//	}
//}

// Creates a new visibility duration for the message.
func (b *BatchHandler) getNewVisibility(e *events.SQSMessage) (int32, error) {
	att, ok := e.Attributes["ApproximateReceiveCount"]
	d, err := strconv.Atoi(att)
	if !ok || err != nil {
		return 0, errors.Join(InvalidReceiveCountError, err)
	}
	nVis := b.calculateBackoff(float64(d))
	return nVis, nil
}

// Calculates an exponential backoff based on the values configured
// in [BackoffSettings] and how many delivery attempts have occurred,
// then adds in jitter in the range set in [BackoffSettings.RandFactor].
// Based on `https://github.com/cenkalti/backoff/blob/v4/exponential.go#L149`
func (b *BatchHandler) calculateBackoff(deliveries float64) int32 {
	s := b.BackOffSettings
	bo := int32(float64(s.InitTimeoutSec) * s.Multiplier * deliveries)

	if s.RandFactor > 0 {
		d := s.RandFactor * float64(bo)
		min := float64(bo) - d
		max := float64(bo) + d
		bo = int32(min + (rand.Float64() * (max - min + 1)))
	}

	if s.MaxTimeoutSec < bo {
		return s.MaxTimeoutSec
	}
	return bo
}

// Requests the original SQS queue to change the message's
// visibility timeout to the provided value.
func (b *BatchHandler) changeVisibility(message *events.SQSMessage, newVisibility int32) error {
	_, err := b.SQSClient.ChangeMessageVisibility(b.Context, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          generateQueueUrl(message.EventSourceARN),
		ReceiptHandle:     &message.ReceiptHandle,
		VisibilityTimeout: newVisibility,
	})

	return err
}

// Builds a queue URL from its ARN
func generateQueueUrl(queueArn string) *string {
	s := strings.Split(queueArn, ":")
	url := fmt.Sprintf("https://sqs.%v.amazonaws.com/%v/%v", s[3], s[4], s[5])
	return &url
}
