package main

import (
	"errors"

	"github.com/aws/aws-lambda-go/events"
)

type WorkReport struct {
	BatchSize, Success, Skip int
	Retry, Failure           RetryFailureReport
	HandlingErrors           []HandlingError
}

type RetryFailureReport struct {
	Count  int
	Errors []MessageReport
}

type MessageReport struct {
	MessageId string
	Error     error
}

func generateReport(event *events.SQSEvent, results map[Status][]Result, errs []HandlingError) (WorkReport, error) {
	report := WorkReport{
		BatchSize:      len(event.Records),
		Success:        len(results[Success]),
		Skip:           len(results[Skip]),
		HandlingErrors: errs,
	}

	hasRetry := results[Retry] != nil
	hasFailure := results[Failure] != nil

	if hasRetry {
		report.Retry = RetryFailureReport{
			Count:  len(results[Retry]),
			Errors: convertReport(results[Retry]),
		}
	}
	if hasFailure {
		report.Failure = RetryFailureReport{
			Count:  len(results[Failure]),
			Errors: convertReport(results[Failure]),
		}
	}

	err := func(hasError bool) error {
		if hasError {
			return errors.New("worker reported errors while processing message batch")
		}
		return nil
	}(hasRetry || hasFailure || len(errs) > 0)

	return report, err
}

func convertReport(results []Result) []MessageReport {
	conv := make([]MessageReport, len(results))
	for i, r := range results {
		conv[i] = MessageReport{
			MessageId: r.Message.MessageId,
			Error:     r.Error,
		}
	}
	return conv
}
