package transport

import (
	"context"
	"errors"
	"fmt"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

func refusalFor(err error) contracts.ApprovalRefusal {
	switch {
	case errors.Is(err, ErrInvalidApproval), errors.Is(err, ErrUnknownApproval):
		return contracts.RefusalInvalid
	case errors.Is(err, ErrApprovalTooLarge):
		return contracts.RefusalTooLarge
	case errors.Is(err, ErrTooManyApprovals):
		return contracts.RefusalTooMany
	case errors.Is(err, context.DeadlineExceeded):
		return contracts.RefusalDeadline
	case errors.Is(err, context.Canceled):
		return contracts.RefusalCancelled
	case errors.Is(err, errStreamClosed):
		return contracts.RefusalStreamClosed
	case errors.Is(err, errSendFailed):
		return contracts.RefusalSendFailed
	}
	return contracts.RefusalUnclassified
}

func refusedAnswer(requestID string, refusal contracts.ApprovalRefusal) (contracts.ApprovalAnswer, error) {
	answer, err := contracts.RefusedAnswer(requestID, refusal)
	if err != nil {
		return contracts.ApprovalAnswer{ID: requestID, Decision: contracts.DecisionDeny}, fmt.Errorf("transport: refuse approval %q: %w", requestID, err)
	}
	return answer, nil
}
