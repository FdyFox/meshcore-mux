// Command meshcore-mux shares one TCP-connected MeshCore companion node
// with multiple companion-protocol clients.
//
// Settings come from built-in defaults, then an optional YAML file
// (--config), then any command-line flags that were set explicitly.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/FdyFox/meshcore-mux/internal/mux"
)

type portList []int

func (p *portList) String() string { return fmt.Sprint(*p) }
func (p *portList) Set(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	*p = append(*p, n)
	return nil
}

type seconds struct{ d *time.Duration }

func (s seconds) String() string {
	if s.d == nil {
		return ""
	}
	return strconv.FormatFloat(s.d.Seconds(), 'f', -1, 64)
}
func (s seconds) Set(v string) error {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return err
	}
	*s.d = time.Duration(f * float64(time.Second))
	return nil
}

func main() {
	os.Exit(run())
}

func run() int {
	mux.SetupLogFromEnv()
	defaults := mux.DefaultConfig()
	// Flags are parsed into their own copy and only explicitly set ones are
	// applied on top of the config file.
	flags := defaults
	var (
		configPath                              string
		probe, version, printConfig             bool
		rejectExport, rejectImport, rejectReset bool
		compatMaintenance, compatAllowExport    bool
		dedicated                               portList
	)
	fs := flag.NewFlagSet("meshcore-mux", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: meshcore-mux [--config FILE] [--upstream-host HOST --upstream-port PORT] [options]")
		fmt.Fprintln(fs.Output(), "Explicit flags override values from the config file.")
		fs.PrintDefaults()
	}
	fs.StringVar(&configPath, "config", "", "YAML configuration `FILE`")
	fs.BoolVar(&printConfig, "print-config", false, "Print the effective configuration as YAML and exit")
	fs.StringVar(&flags.UpstreamHost, "upstream-host", "", "Physical companion host")
	fs.IntVar(&flags.UpstreamPort, "upstream-port", 0, "Physical companion port")
	fs.BoolVar(&probe, "probe", false, "Synchronize and print firmware identification, then exit")
	fs.StringVar(&flags.ListenHost, "listen-host", defaults.ListenHost, "Listener address")
	fs.IntVar(&flags.ListenMultiClientPort, "listen-multi-client-port", defaults.ListenMultiClientPort, "Multi-client listener port")
	fs.Var(&dedicated, "listen-dedicated-client-port", "Dedicated-client listener port (repeatable)")
	fs.IntVar(&flags.OfflineQueueSize, "offline-queue-size", defaults.OfflineQueueSize, "Per-dedicated-client offline queue entries")
	fs.Var(seconds{&flags.ResponseTimeout}, "response-timeout", "Upstream response / contacts idle deadline in `SECONDS`")
	fs.Var(seconds{&flags.ContactsTimeout}, "contacts-timeout", "Total contacts transaction deadline in `SECONDS`")
	fs.Var(seconds{&flags.PollInterval}, "poll-interval", "Inbox fallback polling interval in `SECONDS`")
	fs.BoolVar(&rejectExport, "reject-private-key-export", false, "Reject requester-only private-key export")
	fs.BoolVar(&rejectImport, "reject-private-key-import", false, "Reject private-key import")
	fs.BoolVar(&rejectReset, "reject-factory-reset", false, "Reject factory reset")
	fs.BoolVar(&flags.DeduplicateReceivedMessages, "deduplicate-received-messages", false, "Discard duplicate received text messages")
	// Former opt-in switches, accepted as no-ops so existing command lines keep working.
	fs.BoolVar(&compatMaintenance, "maintenance", false, "Compatibility option; import and reset are allowed by default")
	fs.BoolVar(&compatAllowExport, "allow-private-key-export", false, "Compatibility option; export is allowed by default")
	fs.BoolVar(&version, "version", false, "Show release version")

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if version {
		fmt.Println("meshcore-mux " + mux.Version)
		return 0
	}
	fail := func(err error) int {
		mux.Log.Errorf("process failed: %s", err)
		return 1
	}

	cfg := defaults
	if configPath != "" {
		if err := mux.LoadConfigFile(configPath, &cfg); err != nil {
			return fail(err)
		}
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "upstream-host":
			cfg.UpstreamHost = flags.UpstreamHost
		case "upstream-port":
			cfg.UpstreamPort = flags.UpstreamPort
		case "listen-host":
			cfg.ListenHost = flags.ListenHost
		case "listen-multi-client-port":
			cfg.ListenMultiClientPort = flags.ListenMultiClientPort
		case "listen-dedicated-client-port":
			cfg.ListenDedicatedClientPorts = dedicated
		case "offline-queue-size":
			cfg.OfflineQueueSize = flags.OfflineQueueSize
		case "response-timeout":
			cfg.ResponseTimeout = flags.ResponseTimeout
		case "contacts-timeout":
			cfg.ContactsTimeout = flags.ContactsTimeout
		case "poll-interval":
			cfg.PollInterval = flags.PollInterval
		case "deduplicate-received-messages":
			cfg.DeduplicateReceivedMessages = flags.DeduplicateReceivedMessages
		case "reject-private-key-export":
			cfg.PrivateKeyExport = !rejectExport
		case "reject-private-key-import":
			cfg.PrivateKeyImport = !rejectImport
		case "reject-factory-reset":
			cfg.FactoryReset = !rejectReset
		}
	})

	if printConfig {
		out, err := mux.MarshalConfig(cfg)
		if err != nil {
			return fail(err)
		}
		os.Stdout.Write(out)
		return 0
	}
	mux.Log.Infof("meshcore-mux %s", mux.Version)
	if configPath != "" {
		mux.Log.Infof("event=config.loaded path=%q", configPath)
	}
	if err := cfg.Validate(); err != nil {
		return fail(err)
	}
	if probe {
		id, err := mux.Probe(cfg.UpstreamHost, cfg.UpstreamPort, cfg)
		if err != nil {
			return fail(err)
		}
		fmt.Println(id)
		return 0
	}

	rt := mux.NewRuntime(cfg.UpstreamHost, cfg.UpstreamPort, cfg)
	// Arm the watchdog before Runtime begins work: a daemon unable to reach a
	// running epoch for five minutes is restarted by its supervisor.
	rt.Watchdog = mux.StartWatchdog()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		rt.Stop()
	}()
	if err := rt.Run(); err != nil {
		return fail(err)
	}
	return 0
}
