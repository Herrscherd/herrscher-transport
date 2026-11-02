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

// Announce publishes an Announcement on SubjectAnnounce. It returns the marshal
// or publish error wrapped with a transport prefix; it has no sentinel or typed
// error.
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

// WatchAnnouncements invokes onAnnouncement for every Announcement seen on
// SubjectAnnounce and passes each decode failure to onDecodeErr. Both hooks are
// required: a nil hook is refused with ErrNilHook before any subscription is made.
// It returns the NATS subscribe error.
func WatchAnnouncements(conn *nats.Conn, onAnnouncement func(Announcement), onDecodeErr func(error)) error {
	if onAnnouncement == nil || onDecodeErr == nil {
		return ErrNilHook
	}
	_, err := conn.Subscribe(SubjectAnnounce, func(msg *nats.Msg) {
		var ann Announcement
		if err := Unmarshal(msg.Data, &ann); err != nil {
			onDecodeErr(fmt.Errorf("transport: decode announcement: %w", err))
			return
		}
		onAnnouncement(ann)
	})
	return err
}
