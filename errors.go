package transport

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidFrame     = errors.New("backend: invalid frame (neither event nor done)")
	ErrTLSNotConfigured = errors.New("transport: TLS not configured")
)

type UnknownPortError struct {
	Port string
}

func (e *UnknownPortError) Error() string {
	return fmt.Sprintf("transport: unknown port %q", e.Port)
}

type UnknownMethodError struct {
	Service string
	Method  string
}

func (e *UnknownMethodError) Error() string {
	return fmt.Sprintf("transport: unknown method %s.%s", e.Service, e.Method)
}

type ArgCountError struct {
	Want int
	Got  int
}

func (e *ArgCountError) Error() string {
	return fmt.Sprintf("transport: expected %d args, got %d", e.Want, e.Got)
}

type PlaintextDialError struct {
	Addr string
}

func (e *PlaintextDialError) Error() string {
	return fmt.Sprintf("transport: refusing plaintext dial to non-loopback address %q; configure TLS for remote plugins", e.Addr)
}

type TLSHalfConfiguredError struct {
	Missing []string
}

func (e *TLSHalfConfiguredError) Error() string {
	return fmt.Sprintf("transport: TLS half-configured: %s missing (set CA, cert, and key together, or none for loopback plaintext)", strings.Join(e.Missing, ", "))
}

type CANoCertificateError struct {
	File string
}

func (e *CANoCertificateError) Error() string {
	return fmt.Sprintf("transport: CA %q held no usable certificate", e.File)
}

type remoteError struct {
	message string
}

func (e *remoteError) Error() string {
	return e.message
}
