package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"github.com/Herrscherd/herrscher-contracts/i18n"
	"google.golang.org/grpc"
)

func TestTLSConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     TLSConfig
		wantErr bool
		names   []string
	}{
		{name: "plaintext (none set)", cfg: TLSConfig{}, wantErr: false},
		{name: "full mTLS (all set)", cfg: TLSConfig{CAFile: "ca", CertFile: "c", KeyFile: "k"}, wantErr: false},
		{name: "only CA", cfg: TLSConfig{CAFile: "ca"}, wantErr: true, names: []string{"HERRSCHER_TLS_CERT", "HERRSCHER_TLS_KEY"}},
		{name: "only cert", cfg: TLSConfig{CertFile: "c"}, wantErr: true, names: []string{"HERRSCHER_TLS_CA", "HERRSCHER_TLS_KEY"}},
		{name: "only key", cfg: TLSConfig{KeyFile: "k"}, wantErr: true, names: []string{"HERRSCHER_TLS_CA", "HERRSCHER_TLS_CERT"}},
		{name: "CA+cert, no key", cfg: TLSConfig{CAFile: "ca", CertFile: "c"}, wantErr: true, names: []string{"HERRSCHER_TLS_KEY"}},
		{name: "CA+key, no cert", cfg: TLSConfig{CAFile: "ca", KeyFile: "k"}, wantErr: true, names: []string{"HERRSCHER_TLS_CERT"}},
		{name: "cert+key, no CA", cfg: TLSConfig{CertFile: "c", KeyFile: "k"}, wantErr: true, names: []string{"HERRSCHER_TLS_CA"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateOutcome(t, tc.cfg, tc.wantErr, tc.names)
		})
	}
}

func assertValidateOutcome(t *testing.T, cfg TLSConfig, wantErr bool, names []string) {
	t.Helper()
	err := cfg.Validate()
	if wantErr && err == nil {
		t.Fatalf("Validate() = nil, want a half-configuration error")
	}
	if !wantErr && err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
	if len(names) == 0 {
		return
	}
	half, ok := errors.AsType[*TLSHalfConfiguredError](err)
	if !ok {
		t.Fatalf("Validate() = %v, want *TLSHalfConfiguredError", err)
	}
	for _, n := range names {
		if !slices.Contains(half.Missing, n) {
			t.Fatalf("error %v must name the missing field %q", err, n)
		}
	}
}

func TestTLSHalfConfiguredNotice(t *testing.T) {
	err := TLSConfig{CAFile: "ca"}.Validate()
	assertTextNotice(t, err, contracts.CodeSettingMissing, map[string]string{"Settings": "HERRSCHER_TLS_CERT, HERRSCHER_TLS_KEY"})
}

func TestCAFileNotice(t *testing.T) {
	dir := genCertPair(t)
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := TLSConfig{CAFile: caPath, CertFile: filepath.Join(dir, "server.pem"), KeyFile: filepath.Join(dir, "server-key.pem")}
	_, err := cfg.ServerCredentials()
	assertTextNotice(t, err, contracts.CodeSettingInvalid, map[string]string{"Setting": "HERRSCHER_TLS_CA", "Value": caPath})
}

func TestTLSHalfConfiguredWithoutMissingSettingIsUnexpected(t *testing.T) {
	noticed := (&TLSHalfConfiguredError{}).Notice()
	if noticed.Code != contracts.CodeUnexpected || len(noticed.Args) != 1 || noticed.Args[0].Name() != "Detail" {
		t.Fatalf("notice of an empty half-configuration = %+v, want CodeUnexpected with a Detail", noticed)
	}
}

func TestNoticesRenderInEnglishAndFrench(t *testing.T) {
	dir := genCertPair(t)
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		err     error
		english string
	}{
		{TLSConfig{CAFile: "ca"}.Validate(), "HERRSCHER_TLS_CERT, HERRSCHER_TLS_KEY"},
		{&CAFileError{File: caPath}, "HERRSCHER_TLS_CA"},
		{&PlaintextDialError{Addr: "10.0.0.5:9000"}, "10.0.0.5:9000"},
	}
	renderer := contracts.NewRenderer()
	for _, tc := range cases {
		noticed, ok := errors.AsType[contracts.NoticeError](tc.err)
		if !ok {
			t.Fatalf("error %v carries no notice", tc.err)
		}
		for _, lang := range []i18n.Language{i18n.English, i18n.French} {
			text, renderErr := renderer.Render(lang, noticed.Notice())
			if renderErr != nil || text == "" {
				t.Fatalf("Render(%v) of %v = %q, %v; want a text and no error", lang, tc.err, text, renderErr)
			}
			if lang == i18n.English && !strings.Contains(text, tc.english) {
				t.Fatalf("English render %q does not name %q", text, tc.english)
			}
		}
	}
}

func assertTextNotice(t *testing.T, err error, want contracts.Code, wantArgs map[string]string) {
	t.Helper()
	noticed, ok := errors.AsType[contracts.NoticeError](err)
	if !ok {
		t.Fatalf("error %v carries no notice", err)
	}
	notice := noticed.Notice()
	if notice.Code != want {
		t.Fatalf("notice code = %s, want %s", notice.Code.Key(), want.Key())
	}
	if len(notice.Args) != len(wantArgs) {
		t.Fatalf("notice args = %d, want %d", len(notice.Args), len(wantArgs))
	}
	for _, arg := range notice.Args {
		value, known := wantArgs[arg.Name()]
		if !known || arg.Kind() != contracts.ArgText || arg.Value() != value {
			t.Fatalf("notice arg %q of kind %d = %v, want one of %v as text", arg.Name(), arg.Kind(), arg.Value(), wantArgs)
		}
	}
}

func TestMTLSRoundTrip(t *testing.T) {
	dir := genCertPair(t)
	serverTLS := TLSConfig{CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "server.pem"), KeyFile: filepath.Join(dir, "server-key.pem")}
	clientTLS := TLSConfig{CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "client.pem"), KeyFile: filepath.Join(dir, "client-key.pem")}

	sc, err := serverTLS.ServerCredentials()
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(sc))
	registerMemory(t, newRouter(t, s), &fakeMem{})
	serveInBackground(t, s, lis)
	t.Cleanup(s.Stop)

	cc, err := clientTLS.ClientCredentials()
	if err != nil {
		t.Fatalf("client creds: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mem, err := DialMemory(ctx, RemoteEntry{GrpcAddr: lis.Addr().String()}, cc)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeOrFail(t, mem)
	if err := mem.Record(ctx, contracts.Node{Key: "k"}); err != nil {
		t.Fatalf("mTLS Record should succeed, got %v", err)
	}
}

func TestMTLSRejectsNoClientCert(t *testing.T) {
	dir := genCertPair(t)
	serverTLS := TLSConfig{CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "server.pem"), KeyFile: filepath.Join(dir, "server-key.pem")}

	sc, err := serverTLS.ServerCredentials()
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(sc))
	registerMemory(t, newRouter(t, s), &fakeMem{})
	serveInBackground(t, s, lis)
	t.Cleanup(s.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mem, err := DialMemory(ctx, RemoteEntry{GrpcAddr: lis.Addr().String()}, nil)
	if err != nil {
		return
	}
	defer closeOrFail(t, mem)
	if err := mem.Record(ctx, contracts.Node{Key: "k"}); err == nil {
		t.Fatal("a plaintext client must be rejected by the mTLS server, got a successful Record")
	}
}

func genCertPair(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "herrscher-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	writePEM(t, filepath.Join(dir, "ca.pem"), "CERTIFICATE", caDER)

	leaf := func(name string, eku []x509.ExtKeyUsage, ips []net.IP) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("%s key: %v", name, err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  eku,
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("%s cert: %v", name, err)
		}
		certPath := filepath.Join(dir, name+".pem")
		keyPath := filepath.Join(dir, name+"-key.pem")
		writePEM(t, certPath, "CERTIFICATE", der)
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("%s marshal key: %v", name, err)
		}
		writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	}

	leaf("server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, []net.IP{net.ParseIP("127.0.0.1")})
	leaf("client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil)
	return dir
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer closeOrFail(t, f)
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}
