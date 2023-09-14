package handler

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestValidade_IfNoMessage_ReturnsError(t *testing.T) {
	testVal := Result{
		Status: Success,
	}

	if err := testVal.validate(); err == nil {
		t.Error("empty message passed as valid")
	}
}

func FuzzValidate_InvalidStatusString_ReturnsError(f *testing.F) {
	testcases := []string{"", "\u03b1", "banana", "\n", "_*;/cs\"", "RETRY"}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, a string) {
		r := Result{
			Message: &events.SQSMessage{
				MessageId: "someId",
			},
			Status: Status(a),
		}
		err := r.validate()
		if err == nil &&
			a != string(Failure) &&
			a != string(Retry) &&
			a != string(Skip) &&
			a != string(Success) {
			t.Errorf("Invalid string passed as valid: %v", a)
		}
	})
}
