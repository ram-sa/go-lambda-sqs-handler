package main

// Go lambda functions run on an AL2 custom runtime. You can
// deploy the package using the included script `deploy.sh`

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-lambda-go/events"
	. "github.com/ram-sa/go-sqs-batch-handler"
)

// TODO rearrange to proper file structure
// TODO decide if DeleteUnhandleable is worth it or not
// TODO test functions
// TODO doc everything `https://medium.com/@helbingxxx/how-to-write-go-doc-comments-421e0ca85996`
// TODO learn how to, and then publish the package
// TODO add batching?

func main() {
	//lambda.Start(HandleRequest)
	//simulateEvent()
	//deferTest()
}

/**** Uncomment handler when uploading lambda code
func handleRequest(ctx context.Context, sqsEvent events.SQSEvent) error {
	return nil
}
****/

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
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
	}
	event.Records = append(event.Records, messages...)
	worker := WorkerImp{ExecCeiling: 6}

	sTime := time.Now()
	fmt.Printf("Starting execution at %v\n", sTime.Format(time.TimeOnly))
	fmt.Printf("Execution Ceiling (s): %v\n", worker.ExecCeiling-1)
	fmt.Printf("Messages: %v\n", len(messages))

	b := NewBatchHandler(context.TODO(), "", nil)
	b.HandleEvent(&event, worker)
	//m, _ := HandleEvent(event, WorkerImp{})
	//json, _ := json.MarshalIndent(m, "", "\t")
	//fmt.Println(string(json))

	eTime := time.Now()
	fmt.Printf("Finished execution at %v\n", eTime.Format(time.TimeOnly))
	fmt.Printf("Duration: %v\n", eTime.Sub(sTime))
}

// Type used to test the [Worker] interface
type WorkerImp struct {
	// Defines the ceiling of execution time, in seconds
	ExecCeiling int
}

// Simulates some work and generates a randomized [Report] value,
// including an error if the value is "Failure".
func (w WorkerImp) Work(c context.Context, m *events.SQSMessage) Result {
	reports := []Status{Failure, Retry, Skip, Success}
	r := reports[rand.Intn(len(reports))]
	var e error = nil
	if r == Failure {
		e = errors.New("Errored")
	}
	sDur := rand.Intn(w.ExecCeiling)
	time.Sleep(time.Second * time.Duration(sDur))
	return Result{Message: m, Status: r, Error: e}
}
