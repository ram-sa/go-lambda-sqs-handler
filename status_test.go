package sqshandler

import (
	"testing"
)

func FuzzIsValid_InvalidStatusString_NotOk(f *testing.F) {
	testCases := []string{"", "\u03b1", "banana", "\n", "_*;/cs\"", "RETRY"}
	for _, tc := range testCases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, a string) {
		s := Status(a)
		ok := s.isValid()
		if ok &&
			a != string(Failure) &&
			a != string(Retry) &&
			a != string(Skip) &&
			a != string(Success) {
			t.Errorf("Invalid string passed as valid: %v", a)
		}
	})
}
