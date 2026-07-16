package transport

import (
	"fmt"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"github.com/nats-io/nats.go"
)

// Announcement is what a plugin process publishes at boot: its manifest
// (verbatim contracts.Manifest), where to reach its gRPC server, and a
// per-process identity.
type Announcement struct {
	Manifest   contracts.Manifest
	GrpcAddr   string
	InstanceID string
}

// entry projects the wire Announcement onto the registry's internal RemoteEntry.
// The two are kept as distinct types (wire vs stored) but share a field set, so
// the copy lives here rather than being open-coded at each Observe call.
func (a Announcement) entry() RemoteEntry {
	return RemoteEntry{Manifest: a.Manifest, GrpcAddr: a.GrpcAddr, InstanceID: a.InstanceID}
}

// Announce publishes an Announcement on SubjectAnnounce.
func Announce(nc *nats.Conn, ann Announcement) error {
	b, err := Marshal(ann)
	if err != nil {
		return fmt.Errorf("transport: marshal announcement: %w", err)
	}
	if err := nc.Publish(SubjectAnnounce, b); err != nil {
		return fmt.Errorf("transport: publish announcement: %w", err)
	}
	return nil
}

// WatchAnnouncements invokes fn for every Announcement seen on SubjectAnnounce.
func WatchAnnouncements(nc *nats.Conn, fn func(Announcement)) error {
	_, err := nc.Subscribe(SubjectAnnounce, func(msg *nats.Msg) {
		var ann Announcement
		if err := Unmarshal(msg.Data, &ann); err != nil {
			return
		}
		fn(ann)
	})
	return err
}
