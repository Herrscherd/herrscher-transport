package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

const tinyProto = `syntax = "proto3";

package protogen.test.v1;

option go_package = "example.org/protogentest;protogentest";

message Ping {
  string text = 1;
}

message Pong {
  string text = 1;
}

service Echo {
  rpc Say(Ping) returns (Pong);
}
`

func TestParseArgsRefusesMissingProto(t *testing.T) {
	_, err := parseArgs([]string{"-go_out", ".", "-output", "tiny.pb.go"})
	if !errors.Is(err, errUsage) {
		t.Fatalf("parseArgs without -proto: got %v, want errUsage", err)
	}
}

func TestParseArgsCollectsRepeatedOutputs(t *testing.T) {
	cfg, err := parseArgs([]string{"-proto", "a.proto", "-go_out", ".", "-output", "a.pb.go", "-output", "a_grpc.pb.go"})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if len(cfg.outputs) != 2 || cfg.outputs[0] != "a.pb.go" || cfg.outputs[1] != "a_grpc.pb.go" {
		t.Fatalf("outputs: got %v", cfg.outputs)
	}
}

func TestParseArgsRefusesPositionalArguments(t *testing.T) {
	_, err := parseArgs([]string{"-proto", "a.proto", "-go_out", ".", "-output", "a.pb.go", "extra"})
	if !errors.Is(err, errUsage) {
		t.Fatalf("parseArgs with a positional argument: got %v, want errUsage", err)
	}
}

func TestCheckOutputsRefusesOtherFiles(t *testing.T) {
	name := "tiny.pb.go"
	files := []*pluginpb.CodeGeneratorResponse_File{{Name: proto.String(name)}}
	if err := checkOutputs([]string{"tiny.pb.go", "tiny_grpc.pb.go"}, files); !errors.Is(err, errOutputMismatch) {
		t.Fatalf("checkOutputs with a missing file: got %v, want errOutputMismatch", err)
	}
}

func TestGenerateRefusesUnreadableProto(t *testing.T) {
	cfg := config{
		root:    t.TempDir(),
		proto:   "missing.proto",
		goOut:   t.TempDir(),
		outputs: []string{"missing.pb.go"},
	}
	err := generate(context.Background(), cfg, io.Discard)
	if exitCode(err) != 66 {
		t.Fatalf("exit code of a missing proto: got %d, want 66", exitCode(err))
	}
}

func TestGenerateWritesTheExpectedFiles(t *testing.T) {
	protoPath := filepath.Join(t.TempDir(), "tiny.proto")
	if err := os.WriteFile(protoPath, []byte(tinyProto), 0o644); err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	cfg := config{
		root:    filepath.Dir(protoPath),
		proto:   filepath.Base(protoPath),
		goOut:   outDir,
		outputs: []string{"tiny.pb.go", "tiny_grpc.pb.go"},
	}
	if err := generate(context.Background(), cfg, io.Discard); err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, name := range cfg.outputs {
		info, err := os.Stat(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("expected generated file %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("generated file %s is empty", name)
		}
	}
}

func TestParseArgsDefaultsRootToTheCurrentDirectory(t *testing.T) {
	cfg, err := parseArgs([]string{"-proto", "proto/a.proto", "-go_out", "..", "-output", "a.pb.go"})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if cfg.root != "." {
		t.Fatalf("root without -root: got %q, want \".\"", cfg.root)
	}
	cfg, err = parseArgs([]string{"-root", "..", "-proto", "proto/a.proto", "-go_out", "..", "-output", "a.pb.go"})
	if err != nil {
		t.Fatalf("parseArgs with -root: %v", err)
	}
	if cfg.root != ".." {
		t.Fatalf("root with -root: got %q, want \"..\"", cfg.root)
	}
}

func TestGenerateKeepsTheRegisteredPath(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "proto"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "proto", "tiny.proto"), []byte(tinyProto), 0o644); err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	cfg := config{
		root:    root,
		proto:   "proto/tiny.proto",
		goOut:   outDir,
		outputs: []string{"tiny.pb.go", "tiny_grpc.pb.go"},
	}
	if err := generate(context.Background(), cfg, io.Discard); err != nil {
		t.Fatalf("generate: %v", err)
	}
	generated, err := os.ReadFile(filepath.Join(outDir, "proto", "tiny.pb.go"))
	if err != nil {
		t.Fatalf("expected generated file under the proto directory: %v", err)
	}
	if !strings.Contains(string(generated), "// source: proto/tiny.proto") {
		t.Fatalf("generated file does not register proto/tiny.proto")
	}
}
