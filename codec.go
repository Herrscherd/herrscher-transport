package transport

import "encoding/json"

// Marshal encodes a contracts value for the wire. JSON keeps contracts the
// sole source of truth — no per-type proto.
func Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes a wire payload into a contracts value.
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// encodeArgs encodes a positional argument/result tuple for the wire.
func encodeArgs(vals ...any) ([]byte, error) { return Marshal(vals) }

// decodeArgs decodes a positional tuple into the given destination pointers.
func decodeArgs(data []byte, dst ...any) error { return Unmarshal(data, &dst) }
