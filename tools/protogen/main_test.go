package main

import (
	"bytes"
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
	generated, err := os.ReadFile(filepath.Join(outDir, "tiny.pb.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), "package protogentest") {
		t.Fatalf("tiny.pb.go does not carry the proto package clause")
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

func TestParseArgsRefusesUnknownFlag(t *testing.T) {
	_, err := parseArgs([]string{"-proto", "a.proto", "-go_out", ".", "-output", "a.pb.go", "-bogus", "x"})
	if !errors.Is(err, errUsage) {
		t.Fatalf("parseArgs with an unknown flag: got %v, want errUsage", err)
	}
}

func TestParseArgsRefusesMissingRequiredFlags(t *testing.T) {
	cases := map[string][]string{
		"without -go_out": {"-proto", "a.proto", "-output", "a.pb.go"},
		"without -output": {"-proto", "a.proto", "-go_out", "."},
		"without -proto":  {"-go_out", ".", "-output", "a.pb.go"},
	}
	for name, args := range cases {
		if _, err := parseArgs(args); !errors.Is(err, errUsage) {
			t.Errorf("parseArgs %s: got %v, want errUsage", name, err)
		}
	}
}

func TestExitCodeMapsEachFailureToItsCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 0},
		{"usage", errUsage, 2},
		{"unreadable proto", errUnreadable, 66},
		{"write", errWrite, 74},
		{"compile", errCompile, 1},
		{"plugin", errPlugin, 1},
		{"output mismatch", errOutputMismatch, 1},
	}
	for _, tc := range cases {
		if got := exitCode(tc.err); got != tc.want {
			t.Errorf("exitCode(%s) = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestRunReturnsUsageCodeAndReportsTheFlag(t *testing.T) {
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"-bogus"}, &stderr)
	if code != 2 {
		t.Fatalf("run with an unknown flag: exit %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "-bogus") {
		t.Fatalf("stderr does not name the refused flag: %q", stderr.String())
	}
}

func TestRunReturnsNoInputCodeForAMissingProto(t *testing.T) {
	var stderr bytes.Buffer
	args := []string{"-root", t.TempDir(), "-proto", "missing.proto", "-go_out", t.TempDir(), "-output", "missing.pb.go"}
	if code := run(context.Background(), args, &stderr); code != 66 {
		t.Fatalf("run with a missing proto: exit %d, want 66", code)
	}
	if stderr.Len() == 0 {
		t.Fatalf("run with a missing proto printed nothing on stderr")
	}
}

func TestRunReturnsOneForAProtoThatDoesNotCompile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "broken.proto"), []byte("syntax = \"proto3\";\nmessage {"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	args := []string{"-root", root, "-proto", "broken.proto", "-go_out", t.TempDir(), "-output", "broken.pb.go"}
	if code := run(context.Background(), args, &stderr); code != 1 {
		t.Fatalf("run with a broken proto: exit %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "cannot compile") {
		t.Fatalf("stderr does not name the compile failure: %q", stderr.String())
	}
}

func TestCheckOutputsAcceptsTheExpectedFilesByBaseName(t *testing.T) {
	files := []*pluginpb.CodeGeneratorResponse_File{
		{Name: proto.String("proto/tiny.pb.go")},
		{Name: proto.String("tiny_grpc.pb.go")},
	}
	if err := checkOutputs([]string{"tiny.pb.go", "tiny_grpc.pb.go"}, files); err != nil {
		t.Fatalf("checkOutputs with the expected files: %v", err)
	}
}

func TestCheckOutputsRefusesAFileNobodyAsked(t *testing.T) {
	files := []*pluginpb.CodeGeneratorResponse_File{
		{Name: proto.String("tiny.pb.go")},
		{Name: proto.String("extra.pb.go")},
	}
	if err := checkOutputs([]string{"tiny.pb.go"}, files); !errors.Is(err, errOutputMismatch) {
		t.Fatalf("checkOutputs with an extra file: got %v, want errOutputMismatch", err)
	}
}

func TestWriteFilesWritesEachFileUnderTheOutputDirectory(t *testing.T) {
	outDir := t.TempDir()
	files := []*pluginpb.CodeGeneratorResponse_File{
		{Name: proto.String("proto/tiny.pb.go"), Content: proto.String("package a\n")},
		{Name: proto.String("tiny_grpc.pb.go"), Content: proto.String("package b\n")},
	}
	if err := writeFiles(outDir, files); err != nil {
		t.Fatalf("writeFiles: %v", err)
	}
	cases := map[string]string{
		"proto/tiny.pb.go": "package a\n",
		"tiny_grpc.pb.go":  "package b\n",
	}
	for name, want := range cases {
		got, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestWriteFilesReportsAnOutputDirectoryThatIsAFile(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []*pluginpb.CodeGeneratorResponse_File{{Name: proto.String("tiny.pb.go"), Content: proto.String("package a\n")}}
	err := writeFiles(notADir, files)
	if exitCode(err) != 74 {
		t.Fatalf("writeFiles into a file: exit %d, want 74 (%v)", exitCode(err), err)
	}
}
