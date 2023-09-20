package sqshandler

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

type workReport struct {
	BatchSize     int                 `json:"batchSize"`
	Success       int                 `json:"success,omitempty"`
	Skip          int                 `json:"skip,omitempty"`
	Retry         *retryFailureReport `json:"retry,omitempty"`
	Failure       *retryFailureReport `json:"failure,omitempty"`
	HandlerErrors []errorReport       `json:"handlerErrors,omitempty"`
}

type retryFailureReport struct {
	Count  int           `json:"count"`
	Errors []errorReport `json:"errors"`
}

type errorReport struct {
	MessageId string `json:"messageId"`
	Error     string `json:"error"`
}

func printReport(event *events.SQSEvent, results map[Status][]Result, hErrs []handlerError) {
	report := workReport{
		BatchSize:     len(event.Records),
		Success:       len(results[Success]),
		Skip:          len(results[Skip]),
		HandlerErrors: errorToReport(hErrs),
	}

	if results[Failure] != nil {
		report.Failure = &retryFailureReport{
			Count:  len(results[Failure]),
			Errors: resultToReport(results[Failure]),
		}
	}

	if results[Retry] != nil {
		report.Retry = &retryFailureReport{
			Count:  len(results[Retry]),
			Errors: resultToReport(results[Retry]),
		}
	}

	if json, err := json.Marshal(report); err == nil {
		fmt.Printf("%s\n", json)
	} else {
		fmt.Printf("unable to print report: %v", err)
	}
}

func resultToReport(results []Result) []errorReport {
	conv := make([]errorReport, len(results))
	for i, r := range results {
		conv[i] = errorReport{
			MessageId: r.Message.MessageId,
			Error:     r.Error.Error(),
		}
	}
	return conv
}

func errorToReport(errors []handlerError) []errorReport {
	conv := make([]errorReport, len(errors))
	for i, r := range errors {
		conv[i] = errorReport{
			MessageId: r.MessageId,
			Error:     r.Error.Error(),
		}
	}
	return conv
}
