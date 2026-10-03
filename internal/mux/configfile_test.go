package mux

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestExampleConfigLoadsAndValidates(t *testing.T) {
	cfg := DefaultConfig()
	if err := LoadConfigFile("../../config.example.yaml", &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.UpstreamHost != "192.168.1.50" || cfg.UpstreamPort != 5000 ||
		!slices.Equal(cfg.ListenDedicatedClientPorts, []int{5002, 5003}) || !cfg.DeduplicateReceivedMessages {
		t.Fatalf("unexpected config %+v", cfg)
	}
}

func TestPartialConfigKeepsDefaults(t *testing.T) {
	cfg := DefaultConfig()
	err := parseConfig([]byte("upstream: {host: node.local, port: 5000}\ntiming:\n  response_timeout: 45\n  poll_interval: 1m30s\n"), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	def := DefaultConfig()
	if cfg.ResponseTimeout != 45*time.Second || cfg.PollInterval != 90*time.Second {
		t.Fatalf("durations not applied: %v %v", cfg.ResponseTimeout, cfg.PollInterval)
	}
	if cfg.ContactsTimeout != def.ContactsTimeout || cfg.ListenMultiClientPort != def.ListenMultiClientPort ||
		!cfg.PrivateKeyExport || cfg.ListenHost != def.ListenHost {
		t.Fatalf("defaults lost: %+v", cfg)
	}
}

func TestEmptyConfigFileIsAllowed(t *testing.T) {
	cfg := DefaultConfig()
	if err := parseConfig([]byte("# only comments\n"), &cfg); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsTyposAndBadDurations(t *testing.T) {
	for _, bad := range []string{
		"upstream: {hots: x}\n",
		"timing: {response_timeout: soon}\n",
		"offline_queue_size: many\n",
	} {
		cfg := DefaultConfig()
		if err := parseConfig([]byte(bad), &cfg); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestMarshalConfigRoundTrips(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UpstreamHost, cfg.UpstreamPort = "h", 1
	cfg.ListenDedicatedClientPorts = []int{6000}
	out, err := MarshalConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "response_timeout: 20s") {
		t.Fatalf("durations should be human readable:\n%s", out)
	}
	back := DefaultConfig()
	if err := parseConfig(out, &back); err != nil {
		t.Fatal(err)
	}
	if back.UpstreamHost != "h" || !slices.Equal(back.ListenDedicatedClientPorts, []int{6000}) {
		t.Fatalf("round trip lost values: %+v", back)
	}
}
