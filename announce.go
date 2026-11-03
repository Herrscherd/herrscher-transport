package transport

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
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

const (
	minAnnounceKeyLen = 32
	maxAnnounceBytes  = 32 * 1024
	decimalBase       = 10
)

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
// accepts. The key must come from a cryptographic random source (crypto/rand),
// since a guessable secret lets anyone forge an announcement. The zero value is
// unusable: every operation given it fails with ErrNoAnnounceKey. It prints as a
// fixed mask under fmt and log/slog, and its bytes appear in no formatted output.
type AnnounceKey struct {
	_        [0]func()
	material *announceSecret
}

type announceSecret []byte

// ParseAnnounceKey returns the key for secret, which must hold at least 32 bytes
// and must not be all zero: a shorter or all-zero secret fails with
// ErrWeakAnnounceKey. Generate secret with crypto/rand. The bytes are copied.
func ParseAnnounceKey(secret []byte) (AnnounceKey, error) {
	if len(secret) < minAnnounceKeyLen || !slices.ContainsFunc(secret, nonZeroByte) {
		return AnnounceKey{}, ErrWeakAnnounceKey
	}
	material := announceSecret(bytes.Clone(secret))
	return AnnounceKey{material: &material}, nil
}

func nonZeroByte(b byte) bool { return b != 0 }

// String returns a fixed mask, so that fmt never prints the secret.
func (k AnnounceKey) String() string { return "transport.AnnounceKey(redacted)" }

// GoString returns the same fixed mask as String, for %#v.
func (k AnnounceKey) GoString() string { return k.String() }

// LogValue returns the same fixed mask as String, so that log/slog never
// records the secret.
func (k AnnounceKey) LogValue() slog.Value { return slog.StringValue(k.String()) }

func (a Announcement) entry() RemoteEntry {
	return RemoteEntry(a)
}

type announceEnvelope struct {
	IssuedAtUnixNano int64  `json:"issued_at_unix_nano"`
	Body             []byte `json:"body"`
	MAC              []byte `json:"mac"`
}

func (k AnnounceKey) usable() bool {
	return k.material != nil
}

func (k AnnounceKey) mac(issuedAtUnixNano int64, body []byte) []byte {
	digest := hmac.New(sha256.New, *k.material)
	message := append(strconv.AppendInt(nil, issuedAtUnixNano, decimalBase), '\n')
	message = append(message, body...)
	_, _ = digest.Write(message)
	return digest.Sum(nil)
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
	payload, err := Marshal(announceEnvelope{
		IssuedAtUnixNano: issuedAt,
		Body:             body,
		MAC:              key.mac(issuedAt, body),
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
	if len(payload) > maxAnnounceBytes {
		return Announcement{}, ErrOversizedAnnouncement
	}
	var envelope announceEnvelope
	if err := decodeStrict(payload, &envelope); err != nil {
		return Announcement{}, err
	}
	if !hmac.Equal(envelope.MAC, key.mac(envelope.IssuedAtUnixNano, envelope.Body)) {
		return Announcement{}, ErrUnauthenticatedAnnouncement
	}
	issuedAt := time.Unix(0, envelope.IssuedAtUnixNano)
	if now.Sub(issuedAt).Abs() > AnnounceWindow {
		return Announcement{}, ErrStaleAnnouncement
	}
	var ann Announcement
	if err := decodeStrict(envelope.Body, &ann); err != nil {
		return Announcement{}, err
	}
	return ann, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformedAnnouncement, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after the value", ErrMalformedAnnouncement)
	}
	return nil
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

// WatchAnnouncements subscribes to SubjectAnnounce and invokes onAnnouncement for
// every authentic announcement issued within AnnounceWindow of the local clock.
// Every other message goes to onRejected exactly once, with
// ErrUnauthenticatedAnnouncement, ErrStaleAnnouncement, ErrOversizedAnnouncement
// or ErrMalformedAnnouncement. Both hooks run on the NATS subscription goroutine,
// one call per message, so they must return quickly. Both hooks are required
// (ErrNilHook), and so is a non-zero key (ErrNoAnnounceKey). It returns the
// subscription, which stops delivery when unsubscribed, or the NATS subscribe
// error.
func WatchAnnouncements(conn *nats.Conn, key AnnounceKey, onAnnouncement func(Announcement), onRejected func(error)) (*nats.Subscription, error) {
	return subscribeAnnouncements(conn, key, onAnnouncement, onRejected)
}

func subscribeAnnouncements(conn *nats.Conn, key AnnounceKey, onAnnouncement func(Announcement), onRejected func(error)) (*nats.Subscription, error) {
	if onAnnouncement == nil || onRejected == nil {
		return nil, ErrNilHook
	}
	if !key.usable() {
		return nil, ErrNoAnnounceKey
	}
	return conn.Subscribe(SubjectAnnounce, func(msg *nats.Msg) {
		ann, err := openAnnouncement(key, msg.Data, time.Now())
		if err != nil {
			onRejected(err)
			return
		}
		onAnnouncement(ann)
	})
}
