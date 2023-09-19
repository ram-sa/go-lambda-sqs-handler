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
	SendInvoked, ChangeVisInvoked *int
}

func (m mockSQSClient) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	*m.SendInvoked++
	var err error
	if m.ReturnErrors {
		err = errors.New("mocking generic error response")
	}
	return nil, err
}
func (m mockSQSClient) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	*m.ChangeVisInvoked++
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

//handleFailures

func TestHandleFailures_NoDLQ_DoNothing(t *testing.T) {
	c := 0
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "",
		SQSClient:     mockSQSClient{ReturnErrors: true, SendInvoked: &c},
	}
	failures := []Result{{
		Message: &events.SQSMessage{},
		Status:  Failure,
		Error:   errors.New("error"),
	}}

	err := handler.handleFailures(failures)

	if c > 0 || err != nil {
		t.Error("attempted to send error message to a DLQ")
	}
}

func TestHandleFailures_ErrorOnSend_ReturnHandlingErrors(t *testing.T) {
	c := 0
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "https://sqs.us-east-2.amazonaws.com/444455556666/queue1",
		SQSClient:     mockSQSClient{ReturnErrors: true, SendInvoked: &c},
	}
	failures := []Result{
		{
			Message: &events.SQSMessage{},
			Status:  Failure,
			Error:   errors.New("error1"),
		},
		{
			Message: &events.SQSMessage{},
			Status:  Failure,
			Error:   errors.New("error2"),
		},
	}

	err := handler.handleFailures(failures)

	if c != 2 || len(err) != 2 {
		t.Error("did not return handling errors")
	}
}

func TestHandleFailures_WithDLQ_SendToDLQ(t *testing.T) {
	c := 0
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "https://sqs.us-east-2.amazonaws.com/444455556666/queue1",
		SQSClient:     mockSQSClient{SendInvoked: &c},
	}
	failures := []Result{
		{
			Message: &events.SQSMessage{},
			Status:  Failure,
			Error:   errors.New("error1"),
		},
		{
			Message: &events.SQSMessage{},
			Status:  Failure,
			Error:   errors.New("error2"),
		},
	}

	err := handler.handleFailures(failures)

	if c != 2 || err != nil {
		t.Error("did not send message to DLQ")
	}
}

//getNewVisibility

func TestGetNewVisibility_NoVisibilityAttribute_ReturnsError(t *testing.T) {
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "",
		SQSClient:     mockSQSClient{},
	}
	message := events.SQSMessage{}

	if _, err := handler.getNewVisibility(&message); err == nil {
		t.Error("no error on invalid message attribute")
	}
}

func TestGetNewVisibility_UnableToParseVisibilityAttribute_ReturnsError(t *testing.T) {
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "",
		SQSClient:     mockSQSClient{},
	}
	message := events.SQSMessage{}
	message.Attributes = map[string]string{
		"ApproximateReceiveCount": "invalid",
	}
	if _, err := handler.getNewVisibility(&message); err == nil {
		t.Error("no error on invalid message attribute")
	}
}

func TestGetNewVisibility_AttributeSmalerThanOne_ReturnsError(t *testing.T) {
	handler := BatchHandler{
		Context:       context.TODO(),
		BackOff:       NewBackOff(),
		FailureDlqURL: "",
		SQSClient:     mockSQSClient{},
	}
	message := events.SQSMessage{}
	message.Attributes = map[string]string{
		"ApproximateReceiveCount": "-1",
	}
	if _, err := handler.getNewVisibility(&message); err == nil {
		t.Error("no error on invalid message attribute")
	}
}

// generateQueueUrl

func TestGenerateQueueUrl_InvalidARNString_ReturnError(t *testing.T) {
	invalid := "invalid_string"
	if _, e := generateQueueUrl(invalid); e == nil {
		t.Error("no error on invalid ARN string")
	}
}

func TestGenerateQueueUrl_ValidARNString_ReturnURL(t *testing.T) {
	invalid := "arn:aws:sqs:us-east-2:444455556666:queue1"
	s, e := generateQueueUrl(invalid)
	if e != nil {
		t.Error("error on valid ARN string")
	}
	sVal := *s
	if sVal != "https://sqs.us-east-2.amazonaws.com/444455556666/queue1" {
		t.Error("returned invalid URL")
	}
}
