package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
)

func serveInBackground(tb testing.TB, server *grpc.Server, lis net.Listener) {
	tb.Helper()
	served := make(chan error, 1)
	go func() { served <- server.Serve(lis) }()
	tb.Cleanup(func() {
		server.Stop()
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			tb.Error(err)
		}
	})
}

func listenLoopback(tb testing.TB) net.Listener {
	tb.Helper()
	var listenConfig net.ListenConfig
	lis, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	return lis
}

func closeOrFail(tb testing.TB, closer io.Closer) {
	tb.Helper()
	if err := closer.Close(); err != nil {
		tb.Error(err)
	}
}

func mustMarshal(tb testing.TB, value any) []byte {
	tb.Helper()
	payload, err := Marshal(value)
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}
