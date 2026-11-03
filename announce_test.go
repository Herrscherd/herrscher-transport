package transport

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const announceWait = 5 * time.Second

func runNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	if !srv.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats not ready")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func testKey(tb testing.TB) AnnounceKey {
	tb.Helper()
	key, err := ParseAnnounceKey([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		tb.Fatal(err)
	}
	return key
}

func otherKey(tb testing.TB) AnnounceKey {
	tb.Helper()
	key, err := ParseAnnounceKey([]byte("fedcba9876543210fedcba9876543210"))
	if err != nil {
		tb.Fatal(err)
	}
	return key
}

func sampleAnnouncement() Announcement {
	return Announcement{
		Manifest:   contracts.Manifest{Kind: "sqlite", Category: contracts.CategoryMemory},
		GrpcAddr:   "127.0.0.1:50111",
		InstanceID: "abc",
	}
}

func sealedAt(tb testing.TB, key AnnounceKey, ann Announcement, at time.Time) []byte {
	tb.Helper()
	payload, err := sealAnnouncement(key, ann, at)
	if err != nil {
		tb.Fatalf("seal: %v", err)
	}
	return payload
}

func decodeEnvelope(tb testing.TB, payload []byte) announceEnvelope {
	tb.Helper()
	var envelope announceEnvelope
	if err := Unmarshal(payload, &envelope); err != nil {
		tb.Fatal(err)
	}
	return envelope
}

func unsignedEnvelope(tb testing.TB, ann Announcement) []byte {
	tb.Helper()
	return mustMarshal(tb, announceEnvelope{IssuedAtUnixNano: time.Now().UnixNano(), Body: mustMarshal(tb, ann)})
}

func reencode(tb testing.TB, envelope announceEnvelope) []byte {
	tb.Helper()
	return mustMarshal(tb, envelope)
}

func TestAnnounceWatch(t *testing.T) {
	nc := runNATS(t)
	ctx, cancel := context.WithTimeout(t.Context(), announceWait)
	defer cancel()
	got := make(chan Announcement, 1)
	if _, err := WatchAnnouncements(nc, testKey(t), func(a Announcement) { got <- a }, func(error) {}); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := Announce(nc, sampleAnnouncement(), testKey(t)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	var a Announcement
	select {
	case a = <-got:
	case <-ctx.Done():
		t.Fatalf("no announcement within %s: %v", announceWait, ctx.Err())
	}
	if a.Manifest.Kind != "sqlite" || a.GrpcAddr != "127.0.0.1:50111" {
		t.Fatalf("bad announcement: %+v", a)
	}
}

func TestWatchAnnouncementsRefusesNilHook(t *testing.T) {
	nc := runNATS(t)
	if _, err := WatchAnnouncements(nc, testKey(t), nil, func(error) {}); !errors.Is(err, ErrNilHook) {
		t.Fatalf("nil announcement hook: want ErrNilHook, got %v", err)
	}
	if _, err := WatchAnnouncements(nc, testKey(t), func(Announcement) {}, nil); !errors.Is(err, ErrNilHook) {
		t.Fatalf("nil rejection hook: want ErrNilHook, got %v", err)
	}
}

func TestWatchAnnouncementsRefusesZeroKey(t *testing.T) {
	nc := runNATS(t)
	_, err := WatchAnnouncements(nc, AnnounceKey{}, func(Announcement) {}, func(error) {})
	if !errors.Is(err, ErrNoAnnounceKey) {
		t.Fatalf("zero key: want ErrNoAnnounceKey, got %v", err)
	}
}

func TestAnnounceRefusesZeroKey(t *testing.T) {
	nc := runNATS(t)
	if err := Announce(nc, sampleAnnouncement(), AnnounceKey{}); !errors.Is(err, ErrNoAnnounceKey) {
		t.Fatalf("zero key: want ErrNoAnnounceKey, got %v", err)
	}
}

func TestWatchAnnouncementsReportsDecodeErrors(t *testing.T) {
	nc := runNATS(t)
	ctx, cancel := context.WithTimeout(t.Context(), announceWait)
	defer cancel()
	failures := make(chan error, 1)
	if _, err := WatchAnnouncements(nc, testKey(t), func(Announcement) {}, func(err error) { failures <- err }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := nc.Publish(SubjectAnnounce, []byte("{not json")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	var err error
	select {
	case err = <-failures:
	case <-ctx.Done():
		t.Fatalf("no decode failure within %s: %v", announceWait, ctx.Err())
	}
	if _, ok := errors.AsType[*json.SyntaxError](err); !ok {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWatchAnnouncementsRejectsUnsignedPublish(t *testing.T) {
	nc := runNATS(t)
	ctx, cancel := context.WithTimeout(t.Context(), announceWait)
	defer cancel()
	delivered := make(chan Announcement, 1)
	rejected := make(chan error, 1)
	if _, err := WatchAnnouncements(nc, testKey(t), func(a Announcement) { delivered <- a }, func(err error) { rejected <- err }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := nc.Publish(SubjectAnnounce, unsignedEnvelope(t, sampleAnnouncement())); err != nil {
		t.Fatalf("publish: %v", err)
	}
	var err error
	select {
	case err = <-rejected:
	case <-ctx.Done():
		t.Fatalf("no rejection within %s: %v", announceWait, ctx.Err())
	}
	if !errors.Is(err, ErrUnauthenticatedAnnouncement) {
		t.Fatalf("unsigned publish: want ErrUnauthenticatedAnnouncement, got %v", err)
	}
	if len(delivered) != 0 {
		t.Fatal("unsigned announcement reached the announcement hook")
	}
}

func TestParseAnnounceKeyRefusesShortSecret(t *testing.T) {
	if _, err := ParseAnnounceKey(bytes.Repeat([]byte{1}, 31)); !errors.Is(err, ErrWeakAnnounceKey) {
		t.Fatalf("31 bytes: want ErrWeakAnnounceKey, got %v", err)
	}
}

func TestParseAnnounceKeyAcceptsThirtyTwoBytes(t *testing.T) {
	if _, err := ParseAnnounceKey(bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatalf("32 bytes: want accepted, got %v", err)
	}
}

func TestParseAnnounceKeyRefusesAllZeroSecret(t *testing.T) {
	if _, err := ParseAnnounceKey(make([]byte, 32)); !errors.Is(err, ErrWeakAnnounceKey) {
		t.Fatalf("32 zero bytes: want ErrWeakAnnounceKey, got %v", err)
	}
	if _, err := ParseAnnounceKey(make([]byte, 64)); !errors.Is(err, ErrWeakAnnounceKey) {
		t.Fatalf("64 zero bytes: want ErrWeakAnnounceKey, got %v", err)
	}
}

func TestAnnounceKeyNeverPrintsSecret(t *testing.T) {
	key := testKey(t)
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	logger.Info("announce", "key", key, "any", slog.AnyValue(key))
	outputs := []string{
		fmt.Sprintf("value=%v", key),
		fmt.Sprintf("value=%+v", key),
		fmt.Sprintf("%#v", key),
		fmt.Sprintf("plain=%s", key),
		fmt.Sprintf("quoted=%q", key),
		fmt.Sprintf("%x", key),
		fmt.Sprintf("pointer=%v", &key),
		fmt.Sprintf("%+v", []AnnounceKey{key}),
		fmt.Sprintf("%+v", struct{ Key AnnounceKey }{key}),
		logged.String(),
	}
	forbidden := []string{
		"0123456789abcdef",
		hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"48 49 50 51 52 53 54 55 56 57 97 98 99",
	}
	for _, output := range outputs {
		for _, secret := range forbidden {
			if strings.Contains(output, secret) {
				t.Fatalf("formatted output leaks the key: %q", output)
			}
		}
	}
	if !strings.Contains(logged.String(), "redacted") {
		t.Fatalf("slog output lacks the mask: %q", logged.String())
	}
}

func TestOpenAnnouncementAcceptsFreshSealedAnnouncement(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := sealedAt(t, testKey(t), sampleAnnouncement(), now)
	got, err := openAnnouncement(testKey(t), payload, now.Add(time.Second))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got.InstanceID != "abc" || got.GrpcAddr != "127.0.0.1:50111" || got.Manifest.Kind != "sqlite" {
		t.Fatalf("bad announcement: %+v", got)
	}
}

func TestOpenAnnouncementRefusesUnsignedPayload(t *testing.T) {
	_, err := openAnnouncement(testKey(t), unsignedEnvelope(t, sampleAnnouncement()), time.Now())
	if !errors.Is(err, ErrUnauthenticatedAnnouncement) {
		t.Fatalf("unsigned payload: want ErrUnauthenticatedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesOtherKey(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := sealedAt(t, otherKey(t), sampleAnnouncement(), now)
	_, err := openAnnouncement(testKey(t), payload, now)
	if !errors.Is(err, ErrUnauthenticatedAnnouncement) {
		t.Fatalf("foreign key: want ErrUnauthenticatedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesTamperedBody(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	envelope := decodeEnvelope(t, sealedAt(t, testKey(t), sampleAnnouncement(), now))
	forged := sampleAnnouncement()
	forged.GrpcAddr = "10.0.0.5:9000"
	envelope.Body = mustMarshal(t, forged)
	_, err := openAnnouncement(testKey(t), reencode(t, envelope), now)
	if !errors.Is(err, ErrUnauthenticatedAnnouncement) {
		t.Fatalf("rewritten address: want ErrUnauthenticatedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesRewrittenTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	envelope := decodeEnvelope(t, sealedAt(t, testKey(t), sampleAnnouncement(), now))
	envelope.IssuedAtUnixNano = now.Add(time.Second).UnixNano()
	_, err := openAnnouncement(testKey(t), reencode(t, envelope), now.Add(time.Second))
	if !errors.Is(err, ErrUnauthenticatedAnnouncement) {
		t.Fatalf("rewritten timestamp: want ErrUnauthenticatedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesStaleAnnouncement(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := sealedAt(t, testKey(t), sampleAnnouncement(), now.Add(-AnnounceWindow-time.Second))
	_, err := openAnnouncement(testKey(t), payload, now)
	if !errors.Is(err, ErrStaleAnnouncement) {
		t.Fatalf("stale announcement: want ErrStaleAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesFutureAnnouncement(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := sealedAt(t, testKey(t), sampleAnnouncement(), now.Add(AnnounceWindow+time.Second))
	_, err := openAnnouncement(testKey(t), payload, now)
	if !errors.Is(err, ErrStaleAnnouncement) {
		t.Fatalf("future announcement: want ErrStaleAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementAcceptsExactlyAtWindowEdges(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, skew := range []time.Duration{-AnnounceWindow, AnnounceWindow} {
		payload := sealedAt(t, testKey(t), sampleAnnouncement(), now.Add(skew))
		if _, err := openAnnouncement(testKey(t), payload, now); err != nil {
			t.Fatalf("skew %s: want accepted at the window edge, got %v", skew, err)
		}
	}
}

func TestOpenAnnouncementRefusesJustOutsideWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, skew := range []time.Duration{-AnnounceWindow - time.Nanosecond, AnnounceWindow + time.Nanosecond} {
		payload := sealedAt(t, testKey(t), sampleAnnouncement(), now.Add(skew))
		if _, err := openAnnouncement(testKey(t), payload, now); !errors.Is(err, ErrStaleAnnouncement) {
			t.Fatalf("skew %s: want ErrStaleAnnouncement, got %v", skew, err)
		}
	}
}

func TestOpenAnnouncementSizeBound(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	base := sealedAt(t, testKey(t), sampleAnnouncement(), now)
	atBound := append(bytes.Clone(base), bytes.Repeat([]byte{' '}, maxAnnounceBytes-len(base))...)
	if _, err := openAnnouncement(testKey(t), atBound, now); err != nil {
		t.Fatalf("payload of exactly the bound: want accepted, got %v", err)
	}
	overBound := append(bytes.Clone(atBound), ' ')
	if _, err := openAnnouncement(testKey(t), overBound, now); !errors.Is(err, ErrOversizedAnnouncement) {
		t.Fatalf("payload one byte over the bound: want ErrOversizedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesUnknownEnvelopeField(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var fields map[string]any
	if err := Unmarshal(sealedAt(t, testKey(t), sampleAnnouncement(), now), &fields); err != nil {
		t.Fatal(err)
	}
	fields["extra"] = true
	_, err := openAnnouncement(testKey(t), mustMarshal(t, fields), now)
	if !errors.Is(err, ErrMalformedAnnouncement) {
		t.Fatalf("unknown envelope field: want ErrMalformedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesUnknownBodyField(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	envelope := decodeEnvelope(t, sealedAt(t, testKey(t), sampleAnnouncement(), now))
	var body map[string]any
	if err := Unmarshal(envelope.Body, &body); err != nil {
		t.Fatal(err)
	}
	body["extra"] = true
	envelope.Body = mustMarshal(t, body)
	envelope.MAC = testKey(t).mac(envelope.IssuedAtUnixNano, envelope.Body)
	_, err := openAnnouncement(testKey(t), reencode(t, envelope), now)
	if !errors.Is(err, ErrMalformedAnnouncement) {
		t.Fatalf("unknown body field under a valid MAC: want ErrMalformedAnnouncement, got %v", err)
	}
}

func TestOpenAnnouncementRefusesTrailingData(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := append(sealedAt(t, testKey(t), sampleAnnouncement(), now), []byte(" {}")...)
	_, err := openAnnouncement(testKey(t), payload, now)
	if !errors.Is(err, ErrMalformedAnnouncement) {
		t.Fatalf("trailing data: want ErrMalformedAnnouncement, got %v", err)
	}
}
