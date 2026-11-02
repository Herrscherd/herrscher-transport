package transport

import (
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

func TestCodecRoundTripNode(t *testing.T) {
	in := contracts.Node{
		Key: "sessions/x", Kind: contracts.KindSession, Title: "X",
		Body: "hi", Links: []contracts.Link{{To: "a", Rel: "depends-on"}},
		Meta: map[string]string{"k": "v"},
	}
	b, err := Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out contracts.Node
	if err := Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Key != in.Key || out.Title != in.Title || len(out.Links) != 1 || out.Meta["k"] != "v" {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

func TestDecodeArgsRejectsArityMismatch(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		dests   int
		wantErr bool
	}{
		{name: "exact", payload: `["k",3]`, dests: 2},
		{name: "missing trailing arg", payload: `["k"]`, dests: 2, wantErr: true},
		{name: "extra arg", payload: `["a","b","c"]`, dests: 2, wantErr: true},
		{name: "empty array for two", payload: `[]`, dests: 2, wantErr: true},
		{name: "null payload", payload: `null`, dests: 1, wantErr: true},
		{name: "not an array", payload: `{"key":"k"}`, dests: 1, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, depth, extra := "", 0, ""
			dst := []any{&key, &depth, &extra}[:tc.dests]
			err := decodeArgs([]byte(tc.payload), dst...)
			if tc.wantErr != (err != nil) {
				t.Fatalf("decodeArgs(%s, %d dests) error = %v, wantErr %v", tc.payload, tc.dests, err, tc.wantErr)
			}
		})
	}
}
