package transport

import (
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

// Announce publishes an Announcement on SubjectAnnounce.
func Announce(nc *nats.Conn, ann Announcement) error {
	b, err := Marshal(ann)
	if err != nil {
		return err
	}
	return nc.Publish(SubjectAnnounce, b)
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
