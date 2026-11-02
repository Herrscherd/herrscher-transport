// Command protogen generates the Go code of one .proto file with protoc-gen-go
// and protoc-gen-go-grpc, without protoc.
//
// It compiles the file with protocompile, with -root as import root, then runs
// the two plugins pinned in the go.mod tool directive. -proto is relative to
// -root and is the path the descriptor is registered under. Each file a plugin
// returns is written under -go_out, and the base names of the files the plugins
// return must be exactly the -output list.
//
// Usage:
//
//	protogen [-root <dir>] -proto <file> -go_out <dir> -output <name> [-output <name>...]
//
// Exit codes:
//
//	0   the files were generated and written
//	1   the compilation or a plugin failed, or the plugins returned other files than -output
//	2   the command line is wrong
//	66  the .proto file cannot be read
//	74  a generated file cannot be written
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const parameter = "paths=source_relative"

const (
	exitUsage   = 2
	exitNoInput = 66
	exitIOError = 74
)

var (
	errUsage          = errors.New("bad command line")
	errUnreadable     = errors.New("cannot read the proto file")
	errCompile        = errors.New("cannot compile the proto file")
	errPlugin         = errors.New("plugin failed")
	errOutputMismatch = errors.New("plugins returned other files than -output")
	errWrite          = errors.New("cannot write the generated file")
)

type config struct {
	root    string
	proto   string
	goOut   string
	outputs []string
}

type outputList []string

func (outputs *outputList) String() string {
	return strings.Join(*outputs, ",")
}

func (outputs *outputList) Set(value string) error {
	*outputs = append(*outputs, value)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	cfg, err := parseArgs(args)
	if err == nil {
		err = generate(ctx, cfg, stderr)
	}
	if err != nil {
		if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
			err = errors.Join(err, writeErr)
		}
	}
	return exitCode(err)
}

func exitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		return exitUsage
	case errors.Is(err, errUnreadable):
		return exitNoInput
	case errors.Is(err, errWrite):
		return exitIOError
	default:
		return 1
	}
}

func parseArgs(args []string) (config, error) {
	var cfg config
	var outputs outputList
	flags := flag.NewFlagSet("protogen", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.root, "root", ".", "the import root -proto is relative to")
	flags.StringVar(&cfg.proto, "proto", "", "the .proto file to generate, relative to -root")
	flags.StringVar(&cfg.goOut, "go_out", "", "the directory the generated files are written to")
	flags.Var(&outputs, "output", "a generated file base name (repeatable)")
	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("%w: %w", errUsage, err)
	}
	if flags.NArg() > 0 {
		return config{}, fmt.Errorf("%w: unexpected argument %q", errUsage, flags.Arg(0))
	}
	if cfg.proto == "" || cfg.goOut == "" || len(outputs) == 0 {
		return config{}, fmt.Errorf("%w: -proto, -go_out and at least one -output are required", errUsage)
	}
	cfg.outputs = outputs
	return cfg, nil
}

func generate(ctx context.Context, cfg config, stderr io.Writer) error {
	request, err := compileRequest(ctx, cfg.root, cfg.proto)
	if err != nil {
		return err
	}
	files, err := runPlugins(ctx, request, stderr)
	if err != nil {
		return err
	}
	if err := checkOutputs(cfg.outputs, files); err != nil {
		return err
	}
	return writeFiles(cfg.goOut, files)
}

func compileRequest(ctx context.Context, root, protoPath string) (*pluginpb.CodeGeneratorRequest, error) {
	if _, err := os.ReadFile(filepath.Join(root, protoPath)); err != nil {
		return nil, fmt.Errorf("%w: %w", errUnreadable, err)
	}
	name := filepath.ToSlash(filepath.Clean(protoPath))
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{root},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	compiled, err := compiler.Compile(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errCompile, err)
	}
	if len(compiled) != 1 {
		return nil, fmt.Errorf("%w: %s", errCompile, name)
	}
	return &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{name},
		Parameter:      proto.String(parameter),
		ProtoFile:      descriptorsInDependencyOrder(compiled[0]),
	}, nil
}

func descriptorsInDependencyOrder(root protoreflect.FileDescriptor) []*descriptorpb.FileDescriptorProto {
	var ordered []*descriptorpb.FileDescriptorProto
	visited := make(map[string]bool)
	var visit func(file protoreflect.FileDescriptor)
	visit = func(file protoreflect.FileDescriptor) {
		if visited[file.Path()] {
			return
		}
		visited[file.Path()] = true
		imports := file.Imports()
		for index := range imports.Len() {
			visit(imports.Get(index).FileDescriptor)
		}
		ordered = append(ordered, protodesc.ToFileDescriptorProto(file))
	}
	visit(root)
	return ordered
}

func runPlugins(ctx context.Context, request *pluginpb.CodeGeneratorRequest, stderr io.Writer) ([]*pluginpb.CodeGeneratorResponse_File, error) {
	input, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPlugin, err)
	}
	var files []*pluginpb.CodeGeneratorResponse_File
	for _, tool := range []string{"protoc-gen-go", "protoc-gen-go-grpc"} {
		produced, err := runPlugin(ctx, tool, input, stderr)
		if err != nil {
			return nil, err
		}
		files = append(files, produced...)
	}
	return files, nil
}

func runPlugin(ctx context.Context, tool string, input []byte, stderr io.Writer) ([]*pluginpb.CodeGeneratorResponse_File, error) {
	var stdout bytes.Buffer
	command := exec.CommandContext(ctx, "go", "tool", tool)
	command.Stdin = bytes.NewReader(input)
	command.Stdout = &stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errPlugin, tool, err)
	}
	response := &pluginpb.CodeGeneratorResponse{}
	if err := proto.Unmarshal(stdout.Bytes(), response); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errPlugin, tool, err)
	}
	if message := response.GetError(); message != "" {
		return nil, fmt.Errorf("%w: %s: %s", errPlugin, tool, message)
	}
	return response.GetFile(), nil
}

func checkOutputs(wanted []string, files []*pluginpb.CodeGeneratorResponse_File) error {
	expected := make(map[string]bool, len(wanted))
	for _, name := range wanted {
		expected[filepath.Base(name)] = true
	}
	produced := make(map[string]bool, len(files))
	for _, file := range files {
		produced[filepath.Base(file.GetName())] = true
	}
	if len(produced) != len(files) || !maps.Equal(expected, produced) {
		return fmt.Errorf("%w: expected %v, got %d files", errOutputMismatch, wanted, len(files))
	}
	return nil
}

func writeFiles(goOut string, files []*pluginpb.CodeGeneratorResponse_File) error {
	for _, file := range files {
		target := filepath.Join(goOut, file.GetName())
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return fmt.Errorf("%w: %w", errWrite, err)
		}
		if err := os.WriteFile(target, []byte(file.GetContent()), 0o600); err != nil {
			return fmt.Errorf("%w: %w", errWrite, err)
		}
	}
	return nil
}
