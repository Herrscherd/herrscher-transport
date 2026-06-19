package transport

import "encoding/json"

// Marshal encodes a contracts value for the wire. JSON keeps contracts the
// sole source of truth — no per-type proto.
func Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes a wire payload into a contracts value.
func Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
