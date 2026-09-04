package transport

import (
	"encoding/json"
	"errors"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

const wireErrorKindBudget = "budget"

type wireError struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data,omitempty"`
}

func encodeWireError(err error) []byte {
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

func decodeWireError(payload []byte, message string) error {
	if len(payload) == 0 {
		return errors.New(message)
	}
	var w wireError
	if err := Unmarshal(payload, &w); err != nil {
		return errors.New(message)
	}
	if w.Kind == wireErrorKindBudget {
		var budget contracts.BudgetError
		if err := Unmarshal(w.Data, &budget); err == nil {
			return &budget
		}
	}
	return errors.New(message)
}
