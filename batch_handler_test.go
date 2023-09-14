package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type mockSQSClient struct {
	ReturnErrors                  bool
	SendInvoked, ChangeVisInvoked int
}

func (m mockSQSClient) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.SendInvoked++
	var err error
	if m.ReturnErrors {
		err = errors.New("mocking generic error response")
	}
	return nil, err
}
func (m mockSQSClient) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	m.ChangeVisInvoked++
	var err error
	if m.ReturnErrors {
		err = errors.New("mocking generic error response")
	}
	return nil, err
}

//workWrapped

type panicWorker struct{}

func (panicWorker) Work(c context.Context, msg events.SQSMessage) Result {
	panic("I'm panicking!")
}

func TestWorkWrapped_OnPanic_ReturnsFailedResult(t *testing.T) {
	c := context.TODO()
	msg := events.SQSMessage{MessageId: "someId"}
	wkr := panicWorker{}
	ch1 := make(chan Result)
	ch2 := make(chan Result)

	defer func() {
		if r := recover(); r != nil {
			t.Error("panic state breaches wrapping")
		}
	}()
	go func() {
		r := <-ch1
		ch2 <- r
	}()

	workWrapped(c, msg, wkr, ch1)

	if r := <-ch2; r.Status != Failure {
		t.Errorf("worker panicked but status does not denote failure (%v)", r.Status)
	} else if r.Error == nil {
		t.Error("worker panicked but no error is present on return")
	}
}

//newVisibilityVal

func TestNewVisibilityVal_NoVisibilityAttribute_ReturnsError(t *testing.T) {
	handler := BatchHandler{
		Context:         context.TODO(),
		BackOffSettings: NewBackOffSettings(),
		FailureDlqURL:   "",
		SQSClient:       mockSQSClient{},
	}
	message := events.SQSMessage{}

	if _, err := handler.newVisibilityVal(&message); err == nil {
		t.Error("no error on invalid message attribute")
	}
}

func TestNewVisibilityVal_InvalidVisibilityAttribute_ReturnsError(t *testing.T) {
	handler := BatchHandler{
		Context:         context.TODO(),
		BackOffSettings: NewBackOffSettings(),
		FailureDlqURL:   "",
		SQSClient:       mockSQSClient{},
	}
	message := events.SQSMessage{}
	message.Attributes = map[string]string{
		"ApproximateReceiveCount": "invalid",
	}
	if _, err := handler.newVisibilityVal(&message); err == nil {
		t.Error("no error on invalid message attribute")
	}
}

//calculateBackOff

