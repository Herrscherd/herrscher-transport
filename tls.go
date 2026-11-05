package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// Environment variables that name the TLS settings an operator sets. The notices
// of a half-configured TLS setup and of an unusable CA file name them.
const (
	EnvTLSCA   = "HERRSCHER_TLS_CA"
	EnvTLSCert = "HERRSCHER_TLS_CERT"
	EnvTLSKey  = "HERRSCHER_TLS_KEY"
)

// TLSConfig points at the PEM files that secure off-loopback transport: a CA to
// verify the peer, plus this host's own certificate and key (presented to the
// peer for mutual TLS). All three empty means plaintext: the single-host
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
// missing pieces, so a typo can never downgrade an intended-secure link to
// plaintext.
//
// It returns *TLSHalfConfiguredError when some but not all of CA, cert and key are
// set, and nil otherwise.
func (c TLSConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}
	var missing []string
	if c.CAFile == "" {
		missing = append(missing, EnvTLSCA)
	}
	if c.CertFile == "" {
		missing = append(missing, EnvTLSCert)
	}
	if c.KeyFile == "" {
		missing = append(missing, EnvTLSKey)
	}
	if len(missing) > 0 {
		return &TLSHalfConfiguredError{Missing: missing}
	}
	return nil
}

func (c TLSConfig) loadCertAndPool() (tls.Certificate, *x509.CertPool, error) {
	if !c.Enabled() {
		return tls.Certificate{}, nil, ErrTLSNotConfigured
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
		return tls.Certificate{}, nil, &CAFileError{File: c.CAFile}
	}
	return cert, pool, nil
}

// ServerCredentials builds gRPC server credentials that present this host's cert
// and require+verify a client cert signed by the CA (mutual TLS). Use it on the
// plugin-host listener when TLS is configured.
// It returns ErrTLSNotConfigured when no TLS field is set, *TLSHalfConfiguredError
// on a partial setup, *CAFileError when the CA file holds no certificate,
// and a wrapped error when the key pair or the CA file cannot be read.
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
// from the dial target and matched against the server cert's SANs. It returns
// the same errors as ServerCredentials: ErrTLSNotConfigured, *TLSHalfConfiguredError,
// *CAFileError, or a wrapped read error.
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
