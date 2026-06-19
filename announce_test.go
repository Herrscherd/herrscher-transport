package transport

import (
	"context"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func runNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestAnnounceWatch(t *testing.T) {
	nc := runNATS(t)
	got := make(chan Announcement, 1)
	if err := WatchAnnouncements(nc, func(a Announcement) { got <- a }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	ann := Announcement{
		Manifest:   contracts.Manifest{Kind: "sqlite", Category: contracts.CategoryMemory},
		GrpcAddr:   "127.0.0.1:50111",
		InstanceID: "abc",
	}
	if err := Announce(nc, ann); err != nil {
		t.Fatalf("announce: %v", err)
	}
	select {
	case a := <-got:
		if a.Manifest.Kind != "sqlite" || a.GrpcAddr != "127.0.0.1:50111" {
			t.Fatalf("bad announcement: %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no announcement received")
	}
	_ = context.Background()
}
