package main

// Go lambda functions run on an AL2 custom runtime. You can
// deploy the package using the included script `deploy.sh`

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	. "github.com/ram-sa/go-sqs-batch-handler"
)

// OK TODO rearrange to proper file structure
// OK TODO decide if DeleteUnhandleable is worth it or not
// TODO Add debugging logs
// TODO test functions
// TODO doc everything `https://medium.com/@helbingxxx/how-to-write-go-doc-comments-421e0ca85996`
// TODO learn how to, and then publish the package

type WorkerTest struct{}

func (w WorkerTest) Work(c context.Context, m events.SQSMessage) Result {
	switch m.Body {
	case "Failure":
		return Result{Message: &m, Status: Failure, Error: errors.New("some error")}
	case "Retry":
		return Result{Message: &m, Status: Retry, Error: errors.New("some transient error")}
	case "Skip":
		return Result{Message: &m, Status: Skip}
	case "Timeout":
		time.Sleep(time.Second * 11)
		return Result{Message: &m, Status: Success}
	default:
		return Result{Message: &m, Status: Success}
	}
}

func main() {
	lambda.Start(handleRequest)
	//simulateEvent()
	//deferTest()
}

/**** Uncomment handler when uploading lambda code ****/
func handleRequest(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	handler := New(ctx)
	return handler.HandleEvent(&sqsEvent, WorkerTest{})
}

/****/

// test stuffs

// Tests a defer during a go routine
func deferTest() {
	ch := make(chan int)
	go recoverMe(ch)
	fmt.Printf("I'm recovered! %v\n", <-ch)
}

func recoverMe(pork chan<- int) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovering from", r)
		}
		pork <- 1
	}()
	panicker()
}

func panicker() {
	log.Panic("I'm panicking!\n")
}

// Simulates an SQSEvent and some work. Alter as needed.
// `https://github.com/aws/aws-lambda-go/blob/main/events/testdata/sqs-event.json`
func simulateEvent() {
	bTime := time.Now()
	fmt.Printf("Initializing at %v\n", bTime.Format(time.TimeOnly))
	event := events.SQSEvent{}
	messages := []events.SQSMessage{
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		//		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		//		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		//		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		//		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
	}
	event.Records = append(event.Records, messages...)
	worker := WorkerImp{ExecCeiling: 6}

	sTime := time.Now()
	fmt.Printf("Starting execution at %v\n", sTime.Format(time.TimeOnly))
	fmt.Printf("Execution Ceiling (s): %v\n", worker.ExecCeiling-1)
	fmt.Printf("Messages: %v\n", len(messages))

	b := New(context.TODO())
	r, e := b.HandleEvent(&event, worker)
	//m, _ := HandleEvent(event, WorkerImp{})
	//json, _ := json.MarshalIndent(m, "", "\t")
	//fmt.Println(string(json))

	eTime := time.Now()
	fmt.Printf("Finished execution at %v\n", eTime.Format(time.TimeOnly))
	fmt.Printf("Duration: %v\n\n\n", eTime.Sub(sTime))

	json, _ := json.MarshalIndent(r, "", "	")
	fmt.Printf("%s\n%v", json, e)
}

// Type used to test the [Worker] interface
type WorkerImp struct {
	// Defines the ceiling of execution time, in seconds
	ExecCeiling int
}

// Simulates some work and generates a randomized [Report] value,
// including an error if the value is "Failure".
func (w WorkerImp) Work(c context.Context, m events.SQSMessage) Result {
	reports := []Status{Failure, Skip, Success}
	r := reports[rand.Intn(len(reports))]
	var e error = nil
	if r == Failure {
		e = errors.New("Errored")
	}
	sDur := rand.Intn(w.ExecCeiling)
	time.Sleep(time.Second * time.Duration(sDur))
	return Result{Message: &m, Status: r, Error: e}
}
