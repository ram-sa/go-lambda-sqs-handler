package sqshandler

import "testing"

//calculateBackOff

func FuzzCalculateBackOff_RespectsUpperBound(f *testing.F) {
	testCases := []uint16{0, 5, 10, 15, 20}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, v uint16) {
		backOff := NewBackOff()
		backOff.MaxTimeoutSec = v

		ret := backOff.calculateBackOff(15)

		if ret > (v + v*uint16(backOff.Multiplier)) {
			t.Errorf("timeout ceiling of %v was breached: %v", v, ret)
		}
	})
}

func FuzzCalculateBackOff_RespectsIfNoJitter(f *testing.F) {
	testCases := []uint16{0, 5, 10, 15, 20}
	for _, tc := range testCases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, v uint16) {
		backOff := NewBackOff()
		backOff.MaxTimeoutSec = v
		backOff.RandFactor = 0

		ret := backOff.calculateBackOff(15)

		if ret > (v) {
			t.Errorf("timeout ceiling of %v was breached: %v", v, ret)
		}
	})
}

func TestCalculateBackOff_RespectsSQSLimit(t *testing.T) {
	backOff := NewBackOff()
	backOff.InitTimeoutSec = SQSMaxVisibility
	backOff.MaxTimeoutSec = SQSMaxVisibility

	ret := backOff.calculateBackOff(1)

	if ret > SQSMaxVisibility {
		t.Errorf("SQS timeout ceiling of %v was breached: %v", SQSMaxVisibility, ret)
	}
}

func TestValidate_ForceSQSCeilingOnInit(t *testing.T) {
	backOff := NewBackOff()
	backOff.InitTimeoutSec = SQSMaxVisibility + 10

	backOff.validate()

	if backOff.InitTimeoutSec > SQSMaxVisibility {
		t.Errorf("SQS timeout ceiling of %v was breached on InitTimeoutSec", SQSMaxVisibility)
	}
}

func TestValidate_ForceSQSCeilingOnMax(t *testing.T) {
	backOff := NewBackOff()
	backOff.MaxTimeoutSec = SQSMaxVisibility + 10

	backOff.validate()

	if backOff.MaxTimeoutSec > SQSMaxVisibility {
		t.Errorf("SQS timeout ceiling of %v was breached on MaxTimeoutSec", SQSMaxVisibility)
	}
}

func TestValidate_ForceMultiplierHigherThanOne(t *testing.T) {
	backOff := NewBackOff()
	backOff.Multiplier = 0.5

	backOff.validate()

	if backOff.Multiplier < 1 {
		t.Errorf("backoff multiplier is smaller than 1: %v", backOff.Multiplier)
	}
}
