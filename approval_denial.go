package transport

import (
	"context"
	"errors"
	"fmt"
)

// DenialReason names why the transport refused an approval request itself, rather
// than the asker deciding against it. Its Key is the text ApprovalAnswer.Reason
// carries: a stable catalog key that the host translates for the human.
type DenialReason uint8

const (
	// DenialInvalid reports a request or answer that is malformed, lacks its ID, repeats
	// the ID of a request still in flight, carries another ID, or carries no valid decision.
	DenialInvalid DenialReason = iota + 1
	// DenialTooLarge reports a request or answer above the approval size bound.
	DenialTooLarge
	// DenialTooMany reports a stream that already holds its maximum of requests in flight.
	DenialTooMany
	// DenialNoAsker reports a turn whose context carries no approval asker.
	DenialNoAsker
	// DenialCancelled reports a request cut because its turn was cancelled.
	DenialCancelled
	// DenialDeadline reports a request cut because the deadline of its turn passed.
	DenialDeadline
	// DenialStreamClosed reports a request cut because the approval stream closed or broke.
	DenialStreamClosed
	// DenialSendFailed reports a request or answer that the stream could not send.
	DenialSendFailed
	// DenialUnclassified reports a refusal whose cause the transport does not classify.
	DenialUnclassified
)

// DenialReasons returns every denial reason in declaration order, so a host can check
// that its catalog translates each Key.
func DenialReasons() []DenialReason {
	return []DenialReason{
		DenialInvalid, DenialTooLarge, DenialTooMany, DenialNoAsker,
		DenialCancelled, DenialDeadline, DenialStreamClosed, DenialSendFailed,
		DenialUnclassified,
	}
}

// Key returns the catalog key of the reason. A value outside the set returns a key that
// no catalog defines, so the mistake is visible.
func (r DenialReason) Key() string {
	switch r {
	case DenialInvalid:
		return "approval.refused.invalid"
	case DenialTooLarge:
		return "approval.refused.too_large"
	case DenialTooMany:
		return "approval.refused.too_many"
	case DenialNoAsker:
		return "approval.refused.no_asker"
	case DenialCancelled:
		return "approval.refused.cancelled"
	case DenialDeadline:
		return "approval.refused.deadline"
	case DenialStreamClosed:
		return "approval.refused.stream_closed"
	case DenialSendFailed:
		return "approval.refused.send_failed"
	case DenialUnclassified:
		return "approval.refused.unclassified"
	}
	return fmt.Sprintf("approval.refused.unknown_%d", r)
}

func denialFor(err error) DenialReason {
	switch {
	case errors.Is(err, ErrInvalidApproval), errors.Is(err, ErrUnknownApproval):
		return DenialInvalid
	case errors.Is(err, ErrApprovalTooLarge):
		return DenialTooLarge
	case errors.Is(err, ErrTooManyApprovals):
		return DenialTooMany
	case errors.Is(err, context.DeadlineExceeded):
		return DenialDeadline
	case errors.Is(err, context.Canceled):
		return DenialCancelled
	case errors.Is(err, errStreamClosed):
		return DenialStreamClosed
	case errors.Is(err, errSendFailed):
		return DenialSendFailed
	}
	return DenialUnclassified
}
