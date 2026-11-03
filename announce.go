package transport

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"github.com/nats-io/nats.go"
)

// AnnounceWindow is how far an announcement's issue time may drift from the
// receiver's clock, in either direction. Plugins re-announce every two seconds,
// so the window tolerates several lost announcements while a captured one stays
// replayable only briefly.
const AnnounceWindow = 10 * time.Second

const minAnnounceKeyLen = 32

const decimalBase = 10

// Announcement is what a plugin process publishes at boot: its manifest
// (verbatim contracts.Manifest), where to reach its gRPC server, and a
// per-process identity.
type Announcement struct {
	Manifest   contracts.Manifest
	GrpcAddr   string
	InstanceID string
}

// AnnounceKey is the shared secret that authenticates announcements. Only the
// processes holding the same key can publish an announcement that a watcher
// accepts. The zero value is unusable: every operation given it fails with
// ErrNoAnnounceKey.
type AnnounceKey struct {
	secret []byte
}

// ParseAnnounceKey returns the key for secret, which must hold at least 32
// bytes; a shorter one fails with ErrWeakAnnounceKey. The bytes are copied.
func ParseAnnounceKey(secret []byte) (AnnounceKey, error) {
	if len(secret) < minAnnounceKeyLen {
		return AnnounceKey{}, ErrWeakAnnounceKey
	}
	return AnnounceKey{secret: append([]byte(nil), secret...)}, nil
}

func (a Announcement) entry() RemoteEntry {
	return RemoteEntry(a)
}

type announceEnvelope struct {
	IssuedAtUnixNano int64  `json:"issued_at_unix_nano"`
	Body             []byte `json:"body"`
	MAC              []byte `json:"mac"`
}

func (k AnnounceKey) usable() bool {
	return len(k.secret) > 0
}

func (k AnnounceKey) mac(issuedAtUnixNano int64, body []byte) ([]byte, error) {
	digest := hmac.New(sha256.New, k.secret)
	message := append(strconv.AppendInt(nil, issuedAtUnixNano, decimalBase), '\n')
	message = append(message, body...)
	if _, err := digest.Write(message); err != nil {
		return nil, fmt.Errorf("transport: compute announcement MAC: %w", err)
	}
	return digest.Sum(nil), nil
}

func sealAnnouncement(key AnnounceKey, ann Announcement, now time.Time) ([]byte, error) {
	if !key.usable() {
		return nil, ErrNoAnnounceKey
	}
	body, err := Marshal(ann)
	if err != nil {
		return nil, fmt.Errorf("transport: marshal announcement: %w", err)
	}
	issuedAt := now.UnixNano()
	mac, err := key.mac(issuedAt, body)
	if err != nil {
		return nil, err
	}
	payload, err := Marshal(announceEnvelope{
		IssuedAtUnixNano: issuedAt,
		Body:             body,
		MAC:              mac,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: marshal announcement: %w", err)
	}
	return payload, nil
}

func openAnnouncement(key AnnounceKey, payload []byte, now time.Time) (Announcement, error) {
	if !key.usable() {
		return Announcement{}, ErrNoAnnounceKey
	}
	var envelope announceEnvelope
	if err := Unmarshal(payload, &envelope); err != nil {
		return Announcement{}, fmt.Errorf("transport: decode announcement: %w", err)
	}
	expected, err := key.mac(envelope.IssuedAtUnixNano, envelope.Body)
	if err != nil {
		return Announcement{}, err
	}
	if !hmac.Equal(envelope.MAC, expected) {
		return Announcement{}, ErrUnauthenticatedAnnouncement
	}
	issuedAt := time.Unix(0, envelope.IssuedAtUnixNano)
	if now.Sub(issuedAt).Abs() > AnnounceWindow {
		return Announcement{}, ErrStaleAnnouncement
	}
	var ann Announcement
	if err := Unmarshal(envelope.Body, &ann); err != nil {
		return Announcement{}, fmt.Errorf("transport: decode announcement: %w", err)
	}
	return ann, nil
}

// Announce publishes ann on SubjectAnnounce, sealed under key. It fails with
// ErrNoAnnounceKey when key is the zero value, before anything is published.
func Announce(conn *nats.Conn, ann Announcement, key AnnounceKey) error {
	payload, err := sealAnnouncement(key, ann, time.Now())
	if err != nil {
		return err
	}
	if err := conn.Publish(SubjectAnnounce, payload); err != nil {
		return fmt.Errorf("transport: publish announcement: %w", err)
	}
	return nil
}

// WatchAnnouncements invokes onAnnouncement for every authentic announcement
// seen on SubjectAnnounce, issued within AnnounceWindow of the local clock. Every
// other message goes to onRejected, with ErrUnauthenticatedAnnouncement,
// ErrStaleAnnouncement or a decode error. Both hooks are required (ErrNilHook),
// and so is a non-zero key (ErrNoAnnounceKey). It returns the NATS subscribe
// error.
func WatchAnnouncements(conn *nats.Conn, key AnnounceKey, onAnnouncement func(Announcement), onRejected func(error)) error {
	if onAnnouncement == nil || onRejected == nil {
		return ErrNilHook
	}
	if !key.usable() {
		return ErrNoAnnounceKey
	}
	_, err := conn.Subscribe(SubjectAnnounce, func(msg *nats.Msg) {
		ann, err := openAnnouncement(key, msg.Data, time.Now())
		if err != nil {
			onRejected(err)
			return
		}
		onAnnouncement(ann)
	})
	return err
}
