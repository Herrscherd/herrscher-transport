package transport

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidFrame is returned by a backend stream when a frame is neither an event nor the final done frame.
var ErrInvalidFrame = errors.New("transport: invalid frame (neither event nor done)")

// ErrTLSNotConfigured is returned when TLS is required but no TLS configuration is set.
var ErrTLSNotConfigured = errors.New("transport: TLS not configured")

var ErrPortRegistered = errors.New("transport: port already registered")

var ErrNilHook = errors.New("transport: nil announcement hook")

// UnknownPortError reports a request addressed to a port that no service serves.
type UnknownPortError struct {
	Port string
}

func (e *UnknownPortError) Error() string {
	return fmt.Sprintf("transport: unknown port %q", e.Port)
}

// UnknownMethodError reports a call to a method that the named service does not expose.
type UnknownMethodError struct {
	Service string
	Method  string
}

func (e *UnknownMethodError) Error() string {
	return fmt.Sprintf("transport: unknown method %s.%s", e.Service, e.Method)
}

// ArgCountError reports a payload that holds Got arguments where Want were expected.
type ArgCountError struct {
	Want int
	Got  int
}

func (e *ArgCountError) Error() string {
	return fmt.Sprintf("transport: expected %d args, got %d", e.Want, e.Got)
}

// PlaintextDialError reports a plaintext dial refused because Addr is not a loopback address and no TLS is configured.
type PlaintextDialError struct {
	Addr string
}

func (e *PlaintextDialError) Error() string {
	return fmt.Sprintf("transport: refusing plaintext dial to non-loopback address %q; configure TLS for remote plugins", e.Addr)
}

// TLSHalfConfiguredError reports a TLS setup that sets some of the CA, certificate and key but not all of them.
// Missing lists the settings that are absent.
type TLSHalfConfiguredError struct {
	Missing []string
}

func (e *TLSHalfConfiguredError) Error() string {
	return fmt.Sprintf("transport: TLS half-configured: %s missing (set CA, cert, and key together, or none for loopback plaintext)", strings.Join(e.Missing, ", "))
}

// CAFileError reports a CA file that holds no usable certificate.
type CAFileError struct {
	File string
}

func (e *CAFileError) Error() string {
	return fmt.Sprintf("transport: CA %q held no usable certificate", e.File)
}

type remoteError struct {
	message string
}

func (e *remoteError) Error() string {
	return e.message
}
