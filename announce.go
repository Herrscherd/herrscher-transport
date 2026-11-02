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

func (a Announcement) entry() RemoteEntry {
	return RemoteEntry(a)
}

// Announce publishes an Announcement on SubjectAnnounce.
func Announce(conn *nats.Conn, ann Announcement) error {
	payload, err := Marshal(ann)
	if err != nil {
		return fmt.Errorf("transport: marshal announcement: %w", err)
	}
	if err := conn.Publish(SubjectAnnounce, payload); err != nil {
		return fmt.Errorf("transport: publish announcement: %w", err)
	}
	return nil
}

// WatchAnnouncements invokes fn for every Announcement seen on SubjectAnnounce.
func WatchAnnouncements(nc *nats.Conn, fn func(Announcement)) error {
	return WatchAnnouncementsFunc(nc, fn, nil)
}

// WatchAnnouncementsFunc invokes onAnnouncement for every Announcement seen on
// SubjectAnnounce and passes each decode failure to onErr. A nil onErr drops
// the failures silently.
func WatchAnnouncementsFunc(nc *nats.Conn, onAnnouncement func(Announcement), onErr func(error)) error {
	_, err := nc.Subscribe(SubjectAnnounce, func(msg *nats.Msg) {
		var ann Announcement
		if err := Unmarshal(msg.Data, &ann); err != nil {
			if onErr != nil {
				onErr(fmt.Errorf("transport: decode announcement: %w", err))
			}
			return
		}
		onAnnouncement(ann)
	})
	return err
}
