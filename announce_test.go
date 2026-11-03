package transport

import (
	"context"
	"encoding/json"
	"errors"
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

func reencode(tb testing.TB, envelope announceEnvelope) []byte {
	tb.Helper()
	return mustMarshal(tb, envelope)
}

func TestAnnounceWatch(t *testing.T) {
	nc := runNATS(t)
	ctx, cancel := context.WithTimeout(t.Context(), announceWait)
	defer cancel()
	got := make(chan Announcement, 1)
	if err := WatchAnnouncements(nc, testKey(t), func(a Announcement) { got <- a }, func(error) {}); err != nil {
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
	if err := WatchAnnouncements(nc, testKey(t), nil, func(error) {}); !errors.Is(err, ErrNilHook) {
		t.Fatalf("nil announcement hook: want ErrNilHook, got %v", err)
	}
	if err := WatchAnnouncements(nc, testKey(t), func(Announcement) {}, nil); !errors.Is(err, ErrNilHook) {
		t.Fatalf("nil rejection hook: want ErrNilHook, got %v", err)
	}
}

func TestWatchAnnouncementsRefusesZeroKey(t *testing.T) {
	nc := runNATS(t)
	err := WatchAnnouncements(nc, AnnounceKey{}, func(Announcement) {}, func(error) {})
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
	if err := WatchAnnouncements(nc, testKey(t), func(Announcement) {}, func(err error) { failures <- err }); err != nil {
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
	if err := WatchAnnouncements(nc, testKey(t), func(a Announcement) { delivered <- a }, func(err error) { rejected <- err }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := nc.Publish(SubjectAnnounce, mustMarshal(t, sampleAnnouncement())); err != nil {
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
	if _, err := ParseAnnounceKey(make([]byte, 31)); !errors.Is(err, ErrWeakAnnounceKey) {
		t.Fatalf("31 bytes: want ErrWeakAnnounceKey, got %v", err)
	}
}

func TestParseAnnounceKeyAcceptsThirtyTwoBytes(t *testing.T) {
	if _, err := ParseAnnounceKey(make([]byte, 32)); err != nil {
		t.Fatalf("32 bytes: want accepted, got %v", err)
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
	_, err := openAnnouncement(testKey(t), mustMarshal(t, sampleAnnouncement()), time.Now())
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
