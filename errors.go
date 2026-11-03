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

// ErrPortRegistered is returned when a skeleton is registered on a port that its router already serves.
var ErrPortRegistered = errors.New("transport: port already registered")

// ErrPluginServiceRegistered is returned by NewPluginRouter when the server already serves the Plugin service.
var ErrPluginServiceRegistered = errors.New("transport: Plugin service already registered on this server")

// ErrNilHook is returned by WatchAnnouncements and Watch when a hook is nil.
var ErrNilHook = errors.New("transport: nil announcement hook")

// ErrNoAnnounceKey is returned when an announcement is sent, watched or opened with the zero AnnounceKey.
var ErrNoAnnounceKey = errors.New("transport: no announce key")

// ErrWeakAnnounceKey is returned by ParseAnnounceKey when the secret is shorter than 32 bytes or holds only zero bytes.
var ErrWeakAnnounceKey = errors.New("transport: announce key shorter than 32 bytes or all zero")

// ErrUnauthenticatedAnnouncement is returned for an announcement whose MAC does not verify under the watcher's key: unsigned, tampered with, or signed with another key.
var ErrUnauthenticatedAnnouncement = errors.New("transport: announcement failed authentication")

// ErrStaleAnnouncement is returned for an authentic announcement whose issue time lies outside AnnounceWindow of the receiver's clock.
var ErrStaleAnnouncement = errors.New("transport: announcement outside the freshness window")

// ErrOversizedAnnouncement is returned for a message larger than the announcement size bound, before it is parsed.
var ErrOversizedAnnouncement = errors.New("transport: announcement exceeds the size bound")

// ErrMalformedAnnouncement is returned for a message that is not a single JSON envelope with no unknown field, or whose body is not a single JSON announcement.
var ErrMalformedAnnouncement = errors.New("transport: malformed announcement")

// ErrUnusableAnnouncement is returned by Watch for an authentic announcement that lacks an instance id, a category, or a host:port gRPC address.
var ErrUnusableAnnouncement = errors.New("transport: announcement lacks an instance id, a category or a host:port address")

// ErrAddrRefused is returned by Watch for an authentic announcement whose gRPC address the allowlist refuses, or when no allowlist is set.
var ErrAddrRefused = errors.New("transport: announced address refused by the allowlist")

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
