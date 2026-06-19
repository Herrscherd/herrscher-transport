package transport

import (
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

func TestCodecRoundTripNode(t *testing.T) {
	in := contracts.Node{Key: "sessions/x", Kind: contracts.KindSession, Title: "X",
		Body: "hi", Links: []contracts.Link{{To: "a", Rel: "depends-on"}},
		Meta: map[string]string{"k": "v"}}
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
