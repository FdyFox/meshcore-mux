package mux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration accepts either a Go duration string ("20s", "1m30s", "500ms") or a
// plain number of seconds (20, 0.5) in the configuration file.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: duration must be a value like \"20s\" or 20", n.Line)
	}
	if secs, err := strconv.ParseFloat(n.Value, 64); err == nil {
		*d = Duration(secs * float64(time.Second))
		return nil
	}
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q (examples: \"20s\", \"500ms\", 20)", n.Line, n.Value)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// fileConfig is the on-disk layout. Every field is pre-filled from the current
// Config, so keys missing from the file keep their default value.
type fileConfig struct {
	Upstream struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"upstream"`
	Listen struct {
		Host                 string `yaml:"host"`
		MultiClientPort      int    `yaml:"multi_client_port"`
		DedicatedClientPorts []int  `yaml:"dedicated_client_ports"`
	} `yaml:"listen"`
	OfflineQueueSize            int  `yaml:"offline_queue_size"`
	DeduplicateReceivedMessages bool `yaml:"deduplicate_received_messages"`
	Security                    struct {
		AllowPrivateKeyExport bool `yaml:"allow_private_key_export"`
		AllowPrivateKeyImport bool `yaml:"allow_private_key_import"`
		AllowFactoryReset     bool `yaml:"allow_factory_reset"`
	} `yaml:"security"`
	Timing struct {
		ResponseTimeout Duration `yaml:"response_timeout"`
		ContactsTimeout Duration `yaml:"contacts_timeout"`
		PollInterval    Duration `yaml:"poll_interval"`
	} `yaml:"timing"`
	Persistence struct {
		Enabled       bool     `yaml:"enabled"`
		StateFile     string   `yaml:"state_file"`
		FlushInterval Duration `yaml:"flush_interval"`
	} `yaml:"persistence"`
	Advanced struct {
		CommandLimit            int      `yaml:"command_limit"`
		CommandAge              Duration `yaml:"command_age"`
		VirtualSyncTimeout      Duration `yaml:"virtual_sync_timeout"`
		InboxEntries            int      `yaml:"inbox_entries"`
		OutputFrames            int      `yaml:"output_frames"`
		ConnectTimeout          Duration `yaml:"connect_timeout"`
		FrameTimeout            Duration `yaml:"frame_timeout"`
		WriteTimeout            Duration `yaml:"write_timeout"`
		StartupTimeout          Duration `yaml:"startup_timeout"`
		SigningTimeout          Duration `yaml:"signing_timeout"`
		RadioUncertaintyTimeout Duration `yaml:"radio_uncertainty_timeout"`
	} `yaml:"advanced"`
}

func (f *fileConfig) from(c *Config) {
	f.Upstream.Host, f.Upstream.Port = c.UpstreamHost, c.UpstreamPort
	f.Listen.Host = c.ListenHost
	f.Listen.MultiClientPort = c.ListenMultiClientPort
	f.Listen.DedicatedClientPorts = append([]int(nil), c.ListenDedicatedClientPorts...)
	f.OfflineQueueSize = c.OfflineQueueSize
	f.DeduplicateReceivedMessages = c.DeduplicateReceivedMessages
	f.Security.AllowPrivateKeyExport = c.PrivateKeyExport
	f.Security.AllowPrivateKeyImport = c.PrivateKeyImport
	f.Security.AllowFactoryReset = c.FactoryReset
	f.Timing.ResponseTimeout = Duration(c.ResponseTimeout)
	f.Timing.ContactsTimeout = Duration(c.ContactsTimeout)
	f.Timing.PollInterval = Duration(c.PollInterval)
	f.Persistence.Enabled = c.PersistenceEnabled
	f.Persistence.StateFile = c.StateFile
	f.Persistence.FlushInterval = Duration(c.StateFlushInterval)
	a := &f.Advanced
	a.CommandLimit, a.InboxEntries, a.OutputFrames = c.CommandLimit, c.InboxEntries, c.OutputFrames
	a.CommandAge = Duration(c.CommandAge)
	a.VirtualSyncTimeout = Duration(c.VirtualSyncTimeout)
	a.ConnectTimeout = Duration(c.ConnectTimeout)
	a.FrameTimeout = Duration(c.FrameTimeout)
	a.WriteTimeout = Duration(c.WriteTimeout)
	a.StartupTimeout = Duration(c.StartupTimeout)
	a.SigningTimeout = Duration(c.SigningTimeout)
	a.RadioUncertaintyTimeout = Duration(c.RadioUncertaintyTimeout)
}

func (f *fileConfig) to(c *Config) {
	c.UpstreamHost, c.UpstreamPort = f.Upstream.Host, f.Upstream.Port
	c.ListenHost = f.Listen.Host
	c.ListenMultiClientPort = f.Listen.MultiClientPort
	c.ListenDedicatedClientPorts = f.Listen.DedicatedClientPorts
	c.OfflineQueueSize = f.OfflineQueueSize
	c.DeduplicateReceivedMessages = f.DeduplicateReceivedMessages
	c.PrivateKeyExport = f.Security.AllowPrivateKeyExport
	c.PrivateKeyImport = f.Security.AllowPrivateKeyImport
	c.FactoryReset = f.Security.AllowFactoryReset
	c.ResponseTimeout = time.Duration(f.Timing.ResponseTimeout)
	c.ContactsTimeout = time.Duration(f.Timing.ContactsTimeout)
	c.PollInterval = time.Duration(f.Timing.PollInterval)
	c.PersistenceEnabled = f.Persistence.Enabled
	c.StateFile = f.Persistence.StateFile
	c.StateFlushInterval = time.Duration(f.Persistence.FlushInterval)
	a := &f.Advanced
	c.CommandLimit, c.InboxEntries, c.OutputFrames = a.CommandLimit, a.InboxEntries, a.OutputFrames
	c.CommandAge = time.Duration(a.CommandAge)
	c.VirtualSyncTimeout = time.Duration(a.VirtualSyncTimeout)
	c.ConnectTimeout = time.Duration(a.ConnectTimeout)
	c.FrameTimeout = time.Duration(a.FrameTimeout)
	c.WriteTimeout = time.Duration(a.WriteTimeout)
	c.StartupTimeout = time.Duration(a.StartupTimeout)
	c.SigningTimeout = time.Duration(a.SigningTimeout)
	c.RadioUncertaintyTimeout = time.Duration(a.RadioUncertaintyTimeout)
}

// LoadConfigFile overlays the YAML file at path onto cfg. Keys absent from
// the file keep their current value; unknown keys are rejected so typos are
// reported instead of silently ignored.
func LoadConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return parseConfig(data, cfg)
}

func parseConfig(data []byte, cfg *Config) error {
	var f fileConfig
	f.from(cfg)
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return friendlyConfigError(err)
	}
	f.to(cfg)
	return nil
}

var unknownFieldRe = regexp.MustCompile(`field (\S+) not found in type .*`)

// friendlyConfigError replaces Go type names in decoder messages with plain wording.
func friendlyConfigError(err error) error {
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return fmt.Errorf("config: %w", err)
	}
	lines := make([]string, len(te.Errors))
	for i, e := range te.Errors {
		lines[i] = unknownFieldRe.ReplaceAllString(e, "unknown key \"$1\"")
	}
	return fmt.Errorf("config: %s", strings.Join(lines, "; "))
}

// MarshalConfig renders cfg in the configuration file layout.
func MarshalConfig(cfg Config) ([]byte, error) {
	var f fileConfig
	f.from(&cfg)
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&f); err != nil {
		return nil, err
	}
	return buf.Bytes(), enc.Close()
}
