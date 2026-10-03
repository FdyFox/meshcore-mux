// Package mux implements a protocol-aware multiplexer that lets several
// MeshCore companion-protocol clients share one TCP-connected companion node.
//
// It is a Go port of https://github.com/compumike/meshcore-tcp-mux (MIT).
package mux

import (
	"errors"
	"strings"
	"time"
)

// Version is the release identity. Builds may override it, for example for
// test images: -ldflags "-X github.com/FdyFox/meshcore-mux/internal/mux.Version=0.5.0-test.abc1234".
// Keep the literal in sync with CHANGELOG.md; the release workflow checks it.
var Version = "0.5.1"

// Config holds listener addresses, queue bounds, deadlines, and permissions.
//
// Queue limits count queued and currently active work. The fixed 176-byte
// payload maximum means entry/frame counts also impose finite byte bounds.
type Config struct {
	UpstreamHost               string
	UpstreamPort               int
	ListenHost                 string
	ListenMultiClientPort      int
	ListenDedicatedClientPorts []int
	OfflineQueueSize           int
	CommandLimit               int
	CommandAge                 time.Duration
	// A virtual sync waits for a qualifying physical inbox pop behind other
	// clients' transactions. Expiry rejects only this wait.
	VirtualSyncTimeout time.Duration
	InboxEntries       int
	OutputFrames       int
	ConnectTimeout     time.Duration
	FrameTimeout       time.Duration
	WriteTimeout       time.Duration
	// openHop companions await radio injection and persistence work before
	// producing ordinary replies, so keep generous headroom.
	ResponseTimeout time.Duration
	ContactsTimeout time.Duration
	StartupTimeout  time.Duration
	SigningTimeout  time.Duration
	// If TCP dies before SENT/ERR, the command may nevertheless have reached
	// the companion. Keep shared radio admission closed for this long.
	RadioUncertaintyTimeout time.Duration
	PollInterval            time.Duration
	// Suppress radio retries that the companion exposes as repeated inbox messages.
	DeduplicateReceivedMessages bool
	PrivateKeyExport            bool
	PrivateKeyImport            bool
	FactoryReset                bool
	// Persistence keeps dedicated-client queues across process restarts.
	PersistenceEnabled bool
	StateFile          string
	// StateFlushInterval bounds how often the state file is rewritten; it is
	// also the maximum amount of queue changes lost on a crash.
	StateFlushInterval time.Duration
}

// DefaultConfig returns the same defaults as the original implementation.
func DefaultConfig() Config {
	return Config{
		ListenHost:              "127.0.0.1",
		ListenMultiClientPort:   5001,
		OfflineQueueSize:        256,
		CommandLimit:            16,
		CommandAge:              15 * time.Second,
		VirtualSyncTimeout:      30 * time.Second,
		InboxEntries:            256,
		OutputFrames:            512,
		ConnectTimeout:          5 * time.Second,
		FrameTimeout:            5 * time.Second,
		WriteTimeout:            5 * time.Second,
		ResponseTimeout:         20 * time.Second,
		ContactsTimeout:         30 * time.Second,
		StartupTimeout:          15 * time.Second,
		SigningTimeout:          30 * time.Second,
		RadioUncertaintyTimeout: 60 * time.Second,
		PollInterval:            5 * time.Second,
		PrivateKeyExport:        true,
		PrivateKeyImport:        true,
		FactoryReset:            true,
		StateFile:               "/var/lib/meshcore-mux/state.json",
		StateFlushInterval:      time.Second,
	}
}

// Validate rejects settings that cannot provide finite, positive resource deadlines.
func (c *Config) Validate() error {
	if c.UpstreamHost == "" || c.UpstreamPort == 0 {
		return errors.New("upstream host and port are required")
	}
	if c.UpstreamPort < 1 || c.UpstreamPort > 65535 {
		return errors.New("upstream port must be between 1 and 65535")
	}
	ports := append([]int{c.ListenMultiClientPort}, c.ListenDedicatedClientPorts...)
	seen := make(map[int]bool, len(ports))
	for _, p := range ports {
		if p < 1 || p > 65535 {
			return errors.New("listen ports must be between 1 and 65535")
		}
		if seen[p] {
			return errors.New("listen ports must be unique")
		}
		seen[p] = true
	}
	for _, n := range []int{c.CommandLimit, c.InboxEntries, c.OfflineQueueSize, c.OutputFrames} {
		if n <= 0 {
			return errors.New("queue budgets must be positive")
		}
	}
	for _, d := range []time.Duration{c.CommandAge, c.VirtualSyncTimeout, c.ConnectTimeout, c.FrameTimeout,
		c.WriteTimeout, c.ResponseTimeout, c.ContactsTimeout, c.StartupTimeout, c.SigningTimeout,
		c.RadioUncertaintyTimeout, c.PollInterval} {
		if d <= 0 {
			return errors.New("deadlines and polling interval must be positive")
		}
	}
	if c.PersistenceEnabled && c.StateFile == "" {
		return errors.New("persistence.state_file must be set when persistence is enabled")
	}
	if c.StateFlushInterval <= 0 {
		return errors.New("persistence.flush_interval must be positive")
	}
	if c.ContactsTimeout < c.ResponseTimeout {
		return errors.New("contacts total timeout must not be shorter than its idle timeout")
	}
	return nil
}

// ListenHostEnv names the environment variable that overrides the built-in
// listen address. The container image sets it to 0.0.0.0, because inside a
// container 127.0.0.1 is unreachable from outside and Docker's port publishing
// controls exposure instead. Native installs keep the safe 127.0.0.1 default.
const ListenHostEnv = "MESHCORE_MUX_LISTEN_HOST"

// ApplyEnv overlays supported environment variables onto cfg. It runs after
// the defaults and before the configuration file, so the file and explicit
// flags still take precedence. lookup is os.LookupEnv outside of tests. It
// returns a description of each applied variable for logging.
func ApplyEnv(cfg *Config, lookup func(string) (string, bool)) []string {
	var applied []string
	if v, ok := lookup(ListenHostEnv); ok && strings.TrimSpace(v) != "" {
		cfg.ListenHost = strings.TrimSpace(v)
		applied = append(applied, ListenHostEnv+"="+cfg.ListenHost)
	}
	return applied
}

var clockOrigin = time.Now()

// Now returns monotonic elapsed time since process start, unaffected by
// wall-clock corrections. All protocol deadlines use this clock.
func Now() time.Duration {
	return time.Since(clockOrigin)
}
