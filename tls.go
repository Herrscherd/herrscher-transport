package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// TLSConfig points at the PEM files that secure off-loopback transport: a CA to
// verify the peer, plus this host's own certificate and key (presented to the
// peer for mutual TLS). All three empty means plaintext — the single-host
// loopback default. Any subset is a half-configuration and is rejected
// (fail-closed) rather than silently serving plaintext.
type TLSConfig struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// Enabled reports whether any TLS field is set, i.e. the operator intends TLS.
func (c TLSConfig) Enabled() bool {
	return c.CAFile != "" || c.CertFile != "" || c.KeyFile != ""
}

// Validate fails closed: plaintext (nothing set) is valid and full mTLS (all
// three set) is valid, but any partial configuration is an error naming the
// missing pieces — so a typo can never downgrade an intended-secure link to
// plaintext.
func (c TLSConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}
	var missing []string
	if c.CAFile == "" {
		missing = append(missing, "CA")
	}
	if c.CertFile == "" {
		missing = append(missing, "cert")
	}
	if c.KeyFile == "" {
		missing = append(missing, "key")
	}
	if len(missing) > 0 {
		return fmt.Errorf("transport: TLS half-configured: %s missing (set CA, cert, and key together, or none for loopback plaintext)", strings.Join(missing, ", "))
	}
	return nil
}

// loadCertAndPool loads this host's keypair and the CA pool shared by both the
// server and client credential builders.
func (c TLSConfig) loadCertAndPool() (tls.Certificate, *x509.CertPool, error) {
	if !c.Enabled() {
		return tls.Certificate{}, nil, fmt.Errorf("transport: TLS not configured")
	}
	if err := c.Validate(); err != nil {
		return tls.Certificate{}, nil, err
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("transport: load keypair: %w", err)
	}
	caPEM, err := os.ReadFile(c.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("transport: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("transport: CA %q held no usable certificate", c.CAFile)
	}
	return cert, pool, nil
}

// ServerCredentials builds gRPC server credentials that present this host's cert
// and require+verify a client cert signed by the CA (mutual TLS). Use it on the
// plugin-host listener when TLS is configured.
func (c TLSConfig) ServerCredentials() (credentials.TransportCredentials, error) {
	cert, pool, err := c.loadCertAndPool()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// ClientCredentials builds gRPC client credentials that present this host's cert
// and verify the server against the CA (mutual TLS). The server name is taken
// from the dial target and matched against the server cert's SANs.
func (c TLSConfig) ClientCredentials() (credentials.TransportCredentials, error) {
	cert, pool, err := c.loadCertAndPool()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}), nil
}
