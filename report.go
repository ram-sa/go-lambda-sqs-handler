package handler

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

type WorkReport struct {
	BatchSize     int                 `json:"batchSize"`
	Success       int                 `json:"success,omitempty"`
	Skip          int                 `json:"skip,omitempty"`
	Retry         *RetryFailureReport `json:"retry,omitempty"`
	Failure       *RetryFailureReport `json:"failure,omitempty"`
	HandlerErrors []ErrorReport       `json:"handlerErrors,omitempty"`
}

type RetryFailureReport struct {
	Count  int           `json:"count"`
	Errors []ErrorReport `json:"errors"`
}

type ErrorReport struct {
	MessageId string `json:"messageId"`
	Error     string `json:"error"`
}

func printReport(event *events.SQSEvent, results map[Status][]Result, hErrs []HandlerError) {
	report := WorkReport{
		BatchSize:     len(event.Records),
		Success:       len(results[Success]),
		Skip:          len(results[Skip]),
		HandlerErrors: errorToReport(hErrs),
	}

	if results[Failure] != nil {
		report.Failure = &RetryFailureReport{
			Count:  len(results[Failure]),
			Errors: resultToReport(results[Failure]),
		}
	}

	if results[Retry] != nil {
		report.Retry = &RetryFailureReport{
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

func resultToReport(results []Result) []ErrorReport {
	conv := make([]ErrorReport, len(results))
	for i, r := range results {
		conv[i] = ErrorReport{
			MessageId: r.Message.MessageId,
			Error:     r.Error.Error(),
		}
	}
	return conv
}

func errorToReport(errors []HandlerError) []ErrorReport {
	conv := make([]ErrorReport, len(errors))
	for i, r := range errors {
		conv[i] = ErrorReport{
			MessageId: r.MessageId,
			Error:     r.Error.Error(),
		}
	}
	return conv
}
