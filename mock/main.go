package main

// Go lambda functions run on an AL2 custom runtime. You can
// deploy the package using the included script `deploy.sh`

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

// TODO rearrange to proper file structure
// TODO test functions
// TODO doc everything `https://medium.com/@helbingxxx/how-to-write-go-doc-comments-421e0ca85996`
// TODO learn to, and then publish package
// TODO test if pointers are faster than values as arguments
// TODO add batching?

func main() {
	//lambda.Start(HandleRequest)
	simulateEvent()
}

// *** Uncomment handler when uploading lambda code *** //
//func handleRequest(ctx context.Context, sqsEvent events.SQSEvent) error {
//	return nil
//}

// test stuffs

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

	b := BatchHandler{}
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
func (w WorkerImp) Work(m *events.SQSMessage) Result {
	reports := []Status{Failure, Retry, Skip, Success}
	r := reports[rand.Intn(len(reports))]
	var e error = nil
	if r == Failure {
		e = errors.New("Errored")
	}
	sDur := rand.Intn(w.ExecCeiling)
	time.Sleep(time.Second * time.Duration(sDur))
	return Result{m, r, e}
}
