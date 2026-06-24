package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"
)

func TestTLSConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     TLSConfig
		wantErr bool
		// substrings the error message must name (the missing pieces)
		names []string
	}{
		{name: "plaintext (none set)", cfg: TLSConfig{}, wantErr: false},
		{name: "full mTLS (all set)", cfg: TLSConfig{CAFile: "ca", CertFile: "c", KeyFile: "k"}, wantErr: false},
		{name: "only CA", cfg: TLSConfig{CAFile: "ca"}, wantErr: true, names: []string{"cert", "key"}},
		{name: "only cert", cfg: TLSConfig{CertFile: "c"}, wantErr: true, names: []string{"CA", "key"}},
		{name: "only key", cfg: TLSConfig{KeyFile: "k"}, wantErr: true, names: []string{"CA", "cert"}},
		{name: "CA+cert, no key", cfg: TLSConfig{CAFile: "ca", CertFile: "c"}, wantErr: true, names: []string{"key"}},
		{name: "CA+key, no cert", cfg: TLSConfig{CAFile: "ca", KeyFile: "k"}, wantErr: true, names: []string{"cert"}},
		{name: "cert+key, no CA", cfg: TLSConfig{CertFile: "c", KeyFile: "k"}, wantErr: true, names: []string{"CA"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want a half-configuration error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			for _, n := range tc.names {
				if !strings.Contains(err.Error(), n) {
					t.Fatalf("error %q must name the missing field %q", err, n)
				}
			}
		})
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
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(sc))
	RegisterMemorySkeleton(s, &fakeMem{})
	go func() { _ = s.Serve(lis) }()
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
	defer mem.Close()
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
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(sc))
	RegisterMemorySkeleton(s, &fakeMem{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	// A client dialing plaintext (nil creds) against a mTLS server must be
	// rejected at the handshake — the call fails rather than leaking onto an
	// unauthenticated link.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mem, err := DialMemory(ctx, RemoteEntry{GrpcAddr: lis.Addr().String()}, nil)
	if err != nil {
		return // dial itself refused — acceptable rejection
	}
	defer mem.Close()
	if err := mem.Record(ctx, contracts.Node{Key: "k"}); err == nil {
		t.Fatal("a plaintext client must be rejected by the mTLS server, got a successful Record")
	}
}

// genCertPair writes a CA, a server leaf (SAN 127.0.0.1), and a client leaf
// (ExtKeyUsage ClientAuth) to a temp dir and returns the dir.
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

	leaf := func(name string, eku []x509.ExtKeyUsage, ips []net.IP) (string, string) {
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
		return certPath, keyPath
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
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}
