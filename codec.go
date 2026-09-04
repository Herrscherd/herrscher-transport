package transport

import (
	"encoding/json"
	"fmt"
)

// Marshal encodes a contracts value for the wire. JSON keeps contracts the
func Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes a wire payload into a contracts value.
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// encodeArgs encodes a positional argument/result tuple for the wire.
func encodeArgs(vals ...any) ([]byte, error) { return Marshal(vals) }

// decodeArgs decodes a positional tuple into the given destination pointers.
func decodeArgs(data []byte, dst ...any) error {
	var raw []json.RawMessage
	if err := Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("transport: decode args: %w", err)
	}
	if len(raw) != len(dst) {
		return fmt.Errorf("transport: expected %d args, got %d", len(dst), len(raw))
	}
	for i := range dst {
		if err := Unmarshal(raw[i], dst[i]); err != nil {
			return fmt.Errorf("transport: decode arg %d: %w", i, err)
		}
	}
	return nil
}
