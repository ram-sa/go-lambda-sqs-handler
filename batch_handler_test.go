package sqshandler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type mockSQSClient struct {
	ReturnErrors                  bool
	SendInvoked, ChangeVisInvoked *int
}

func (m mockSQSClient) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	var err error
	if m.SendInvoked != nil {
		*m.SendInvoked++
	}
	if m.ReturnErrors {
		err = errors.New("mocking generic error response")
	}
	return nil, err
}

func (m mockSQSClient) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	var err error
	if m.ChangeVisInvoked != nil {
		*m.ChangeVisInvoked++
	}
	if m.ReturnErrors {
		err = errors.New("mocking generic error response")
	}
	return nil, err
}

//HandleEvent

//setTimer

func TestSetTimer_NoDeadlineOnContext_DefaultToFifteen(t *testing.T) {
	ctx := context.TODO()
	want := time.Now().Add(15 * time.Minute).Add(-5 * time.Second)

	timer, deadline := setTimer(ctx)

	if !(deadline.Before(want.Add(50*time.Millisecond)) &&
		deadline.After(want.Add(-50*time.Millisecond))) {
		t.Error("default deadline not set to 15 minutes")
	}

	timer.Stop()
}

func TestSetTimer_ReserveFiveSeconds(t *testing.T) {
	want := time.Now().Add(5 * time.Minute)
	ctx, cf := context.WithDeadline(context.TODO(), want)
	want = want.Add(-5 * time.Second)

	timer, deadline := setTimer(ctx)

	if !(deadline.Before(want.Add(50*time.Millisecond)) &&
		deadline.After(want.Add(-50*time.Millisecond))) {
		t.Error("deadline did not reserve 5 seconds")
	}

	timer.Stop()
	cf()
}

//workWrapped

type panicker struct{}

func (panicker) Work(c context.Context, msg events.SQSMessage) Result {
	panic("I'm panicking!")
}

func TestWorkWrapped_OnPanic_ReturnsFailedResult(t *testing.T) {
	c := context.TODO()
	msg := events.SQSMessage{MessageId: "someId"}
	wkr := panicker{}
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

//handleResults

func TestHandleResults_MultipleHandlingErrors_AggregateAndReturn(t *testing.T) {
	handler := BatchHandler{
		BackOff:       NewBackOff(),
		FailureDlqURL: "arn:aws:sqs:us-east-2:444455556666:queue1",
		SQSClient:     mockSQSClient{ReturnErrors: true},
	}
	m1 := events.SQSMessage{
		MessageId:      "id1",
		EventSourceARN: "arn:aws:sqs:us-east-2:444455556666:queue1",
		Attributes:     map[string]string{"ApproximateReceiveCount": "1"},
	}
	m2 := events.SQSMessage{
		MessageId:      "id2",
		EventSourceARN: "arn:aws:sqs:us-east-1:444455556666:queue2",
		Attributes:     map[string]string{"ApproximateReceiveCount": "3"},
	}
	results := map[Status][]Result{
		Retry:   {{Message: &m1, Status: Retry}},
		Failure: {{Message: &m2, Status: Failure}},
	}

	_, err := handler.handleResults(results)

	if len(err) < 2 {
		t.Error("did not aggregate handling errors")
	}
}

func TestHandleResults_HasRetries_ReturnMessageIds(t *testing.T) {
	handler := BatchHandler{BackOff: NewBackOff(), SQSClient: mockSQSClient{}}
	m1 := events.SQSMessage{
		MessageId:      "id1",
		EventSourceARN: "arn:aws:sqs:us-east-2:444455556666:queue1",
		Attributes:     map[string]string{"ApproximateReceiveCount": "1"},
	}
	m2 := events.SQSMessage{
		MessageId:      "id2",
		EventSourceARN: "arn:aws:sqs:us-east-1:444455556666:queue2",
		Attributes:     map[string]string{"ApproximateReceiveCount": "3"},
	}
	results := map[Status][]Result{
		Retry:   {{Message: &m1, Status: Retry}},
		Failure: {{Message: &m2, Status: Failure}},
	}

	event, err := handler.handleResults(results)

	if err != nil || len(event.BatchItemFailures) != 1 {
		t.Error("did not return message ids")
	}
}

//handleRetries

func TestHandleRetries_ErrorOnGetNewVisibility_ReturnHandlingErrors(t *testing.T) {
	handler := BatchHandler{
		BackOff: NewBackOff(),
	}
	message := events.SQSMessage{MessageId: "id"}
	message.Attributes = map[string]string{
		"ApproximateReceiveCount": "invalid",
	}
	retries := []Result{{
		Message: &message,
		Status:  Retry,
	}}

	err, _ := handler.handleRetries(retries)

	if len(err) != 1 {
		t.Error("did not return handling errors")
	}
}

func TestHandleRetries_ErrorOnChangeVisibility_ReturnHandlingErrors(t *testing.T) {
	c := 0
	handler := BatchHandler{
		BackOff:   NewBackOff(),
		SQSClient: mockSQSClient{ReturnErrors: true, ChangeVisInvoked: &c},
	}
	message := events.SQSMessage{
		MessageId:      "id",
		EventSourceARN: "arn:aws:sqs:us-east-2:444455556666:queue1",
	}
	message.Attributes = map[string]string{
		"ApproximateReceiveCount": "1",
	}
	retries := []Result{{
		Message: &message,
		Status:  Retry,
	}}

	err, _ := handler.handleRetries(retries)
	if c == 0 || len(err) != 1 {
		t.Error("did not return handling errors")
	}
}

func TestHandleRetries_VisibilityChanged_ReturnBatchItemEvent(t *testing.T) {
	c := 0
	handler := BatchHandler{BackOff: NewBackOff(), SQSClient: mockSQSClient{ChangeVisInvoked: &c}}
	m1 := events.SQSMessage{
		MessageId:      "id1",
		EventSourceARN: "arn:aws:sqs:us-east-2:444455556666:queue1",
		Attributes:     map[string]string{"ApproximateReceiveCount": "1"},
	}
	m2 := events.SQSMessage{
		MessageId:      "id2",
		EventSourceARN: "arn:aws:sqs:us-east-1:444455556666:queue2",
		Attributes:     map[string]string{"ApproximateReceiveCount": "3"},
	}
	retries := []Result{
		{Message: &m1, Status: Retry},
		{Message: &m2, Status: Retry},
	}

	event, err := handler.handleRetries(retries)

	if c < 2 || err != nil || len(event) < 2 {
		t.Error("did not return handling errors")
	}
}

//handleFailures

func TestHandleFailures_NoDLQ_DoNothing(t *testing.T) {
	c := 0
	handler := BatchHandler{
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
	valid := "arn:aws:sqs:us-east-2:444455556666:queue1"
	s, e := generateQueueUrl(valid)
	if e != nil {
		t.Error("error on valid ARN string")
	}
	sVal := *s
	if sVal != "https://sqs.us-east-2.amazonaws.com/444455556666/queue1" {
		t.Error("returned invalid URL")
	}
}
