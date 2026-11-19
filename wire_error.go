package transport

import (
	"encoding/json"
	"errors"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

const (
	wireErrorKindBudget           = "budget"
	wireErrorKindTooManyApprovals = "approval_too_many"
	wireErrorKindInvalidApproval  = "approval_invalid"
	wireErrorKindApprovalTooLarge = "approval_too_large"
)

type wireError struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data,omitempty"`
}

func encodeWireError(err error) []byte {
	if payload := encodeBudgetError(err); payload != nil {
		return payload
	}
	return encodeApprovalError(err)
}

func encodeBudgetError(err error) []byte {
	var budget *contracts.BudgetError
	if !errors.As(err, &budget) {
		return nil
	}
	data, marshalErr := Marshal(budget)
	if marshalErr != nil {
		return nil
	}
	payload, marshalErr := Marshal(wireError{Kind: wireErrorKindBudget, Data: data})
	if marshalErr != nil {
		return nil
	}
	return payload
}

func encodeApprovalError(err error) []byte {
	kind := approvalWireKindOf(err)
	if kind == "" {
		return nil
	}
	payload, marshalErr := Marshal(wireError{Kind: kind})
	if marshalErr != nil {
		return nil
	}
	return payload
}

func approvalWireKindOf(err error) string {
	switch {
	case errors.Is(err, ErrTooManyApprovals):
		return wireErrorKindTooManyApprovals
	case errors.Is(err, ErrInvalidApproval):
		return wireErrorKindInvalidApproval
	case errors.Is(err, ErrApprovalTooLarge):
		return wireErrorKindApprovalTooLarge
	}
	return ""
}

func approvalSentinelOf(kind string) error {
	switch kind {
	case wireErrorKindTooManyApprovals:
		return ErrTooManyApprovals
	case wireErrorKindInvalidApproval:
		return ErrInvalidApproval
	case wireErrorKindApprovalTooLarge:
		return ErrApprovalTooLarge
	}
	return nil
}

func decodeWireError(payload []byte, message string) error {
	if len(payload) == 0 {
		return &remoteError{message: message}
	}
	var wire wireError
	if err := Unmarshal(payload, &wire); err != nil {
		return &remoteError{message: message}
	}
	if wire.Kind == wireErrorKindBudget {
		var budget contracts.BudgetError
		if err := Unmarshal(wire.Data, &budget); err == nil {
			return &budget
		}
	}
	if sentinel := approvalSentinelOf(wire.Kind); sentinel != nil {
		return sentinel
	}
	return &remoteError{message: message}
}
