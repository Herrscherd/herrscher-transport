package transport

import (
	"encoding/json"
	"fmt"
)

// Marshal encodes a contracts value for the wire as JSON.
func Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes a wire payload into a contracts value.
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func encodeArgs(vals ...any) ([]byte, error) { return Marshal(vals) }

func decodeArgs(data []byte, dst ...any) error {
	var raw []json.RawMessage
	if err := Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("transport: decode args: %w", err)
	}
	if len(raw) != len(dst) {
		return &ArgCountError{Want: len(dst), Got: len(raw)}
	}
	for index := range dst {
		if err := Unmarshal(raw[index], dst[index]); err != nil {
			return fmt.Errorf("transport: decode arg %d: %w", index, err)
		}
	}
	return nil
}
