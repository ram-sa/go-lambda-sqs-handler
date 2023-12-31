package sqshandler

import (
	"fmt"
	"math"
	"math/rand"
)

/*
BackOff defines values used for calculating a message's exponential
backoff in case of a transient failure by altering its visibility timeout.
Each retry attempt will take exponentially longer based on the amount of
delivery attempts (attribute [ApproximateReceiveCount]) until the
message is  either delivered or sent to a DLQ, according to the
following parameters:

# InitTimeoutSec

Defines the initial timeout value for the message, in seconds,
ranging from 0 to 43200 (12h, the maximum value accepted by AWS).

# MaxTimeoutSec

Defines the maximum timeout value for the message, in seconds,
ranging from 0 to 43200 (12h, the maximum value accepted by AWS).
Note that this does not include jitter ranges, unless the final value exceeds 43200.

For example, if MaxTimeoutSec is set to 6, and RandFactor is set to 0.5, the final
timeout value can be any integer from 3 to 9. However if MaxTimeoutSec is set to 43200,
the values will range from 21600 to 43200 (instead of 64800).

# Multiplier

Defines the scaling factor of the exponential function. Note that the
resulting values will be rounded down, as AWS only accepts positive integer values.
Setting this value to 1 will linearize the backoff curve.

# RandFactor

Adds a jitter factor to the function by making it so
that the final timeout value ranges in [interval * (1 ± RandFactor)], rounded down.
Setting this value to 0 disables it.

# Example

For the default values 5, 300, 2.5 and 0.2:

	D	Timeout		Timeout		Timeout
	#	(Raw)		(NoJitter)	(WithJitter)

	1	5   		5   		[4, 6]
	2	12.5		12  		[9, 14]
	3	31.25		31  		[24, 37]
	4	78.125		78  		[62, 93]
	5	195.3125	195 		[156, 234]
	6	488.28125	300 		[240, 360]
	7	1220.7031	300 		[240, 360]

Based on `https://github.com/cenkalti/backoff/blob/v4/exponential.go`.

For more information about message visibility, see:
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html.

[ApproximateReceiveCount]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestSyntax
*/
type BackOff struct {
	InitTimeoutSec, MaxTimeoutSec uint16
	Multiplier, RandFactor        float64
}

// Default values for BackOff.
const (
	DefaultIniTimeout = 5
	DefaultMaxTimeout = 300
	DefaultMultiplier = 2.5
	DefaultRandFactor = 0.3
)

// SQS maximum value for message visibility
const SQSMaxVisibility = 43200

// NewBackoff creates an instance of BackOff using default values.
func NewBackOff() BackOff {
	return BackOff{
		InitTimeoutSec: DefaultIniTimeout,
		MaxTimeoutSec:  DefaultMaxTimeout,
		Multiplier:     DefaultMultiplier,
		RandFactor:     DefaultRandFactor,
	}
}

/*
Calculates an exponential backoff using the values set
in BackOff and how many delivery attempts have occurred:

	initTimeout * multiplier^(deliveries-1)

Then returns a random value from the interval:

	[timeout - randFactor * timeout, timeout + randFactor * timeout]

as long as randFactor > 0.

Based on `https://github.com/cenkalti/backoff/blob/v4/exponential.go#L149`
with additional, SQS specific constrains.
*/
func (s *BackOff) calculateBackOff(deliveries float64) uint16 {
	bo := uint16(float64(s.InitTimeoutSec) * math.Pow(s.Multiplier, deliveries-1))

	if s.MaxTimeoutSec < bo {
		bo = s.MaxTimeoutSec
	}

	if s.RandFactor > 0 {
		d := s.RandFactor * float64(bo)
		min := float64(bo) - d
		max := float64(bo) + d
		bo = uint16(min + (rand.Float64() * (max - min + 1)))
	}

	if bo > SQSMaxVisibility {
		bo = SQSMaxVisibility
	}

	return bo
}

// Ensures values are within acceptable SQS ranges. This function should
// be called at least once before a call to calculateBackOff.
func (b *BackOff) validate() {
	if b.InitTimeoutSec > SQSMaxVisibility {
		fmt.Printf("warning: InitTimeoutSec exceeds SQS maximum. Defaulting to %v.\n", SQSMaxVisibility)
		b.InitTimeoutSec = SQSMaxVisibility
	} else if b.MaxTimeoutSec > SQSMaxVisibility {
		fmt.Printf("warning: MaxTimeoutSec exceeds SQS maximum. Defaulting to %v.\n", SQSMaxVisibility)
		b.MaxTimeoutSec = SQSMaxVisibility
	} else if b.Multiplier < 1 {
		fmt.Println("warning: Multiplier value smaller than 1. Defaulting to 1.")
		b.Multiplier = 1
	}
}
