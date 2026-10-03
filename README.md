# MeshCore Mux: share one companion node with multiple apps

Run [MeshCore-HA](https://github.com/meshcore-dev/meshcore-ha), `meshcore-cli`, desktop and phone apps, and bots at the same time, all sharing a single TCP-connected MeshCore companion. `meshcore-mux` is a small, protocol-aware multiplexer: it speaks the full companion protocol so multiple clients can safely share one node.

This is a Go port of [compumike/meshcore-tcp-mux](https://github.com/compumike/meshcore-tcp-mux) (Crystal, MIT). It has its own name to avoid confusion with the original, but its command-line flags are identical, so existing command lines work unchanged. It adds a YAML configuration file and more transparent logging.

```text
 meshcore-cli --\
 meshcore-cli ---+--> :5001 multi-client ----\
 misc one-off --/                            |
                                             +--> meshcore-mux --> your_companion:5000
 MeshCore-HA -------> :5002 dedicated -------|
 desktop app -------> :5003 dedicated -------|
 phone app ---------> :5004 dedicated ------/

                many listener ports                     one upstream TCP connection
```

All clients share the companion's identity, contacts, channels, and radio settings. All clients can send and receive messages, but they do not see *outgoing* messages sent by other clients (see [LIMITATIONS.md](LIMITATIONS.md)).

## Why not a plain TCP proxy?

The companion protocol has no request IDs: an `OK` or `ERR` does not say which client it belongs to, and `SYNC_NEXT_MESSAGE` removes a message from the device's single inbox. Naively forwarding bytes would deliver replies to the wrong client and split incoming messages between clients. The mux therefore:

- serializes commands round-robin and returns each reply only to the client that asked,
- is the only consumer of the companion's inbox and copies every received message to every client,
- reserves shared firmware state (DM acknowledgement ring, remote login/status/trace requests, signing) and routes later radio results to the right client,
- virtualizes flood scope and protocol version per client,
- never replays a possibly executed command, and reconnects with backoff.

Details are in [ARCHITECTURE.md](ARCHITECTURE.md).

## Multi-client vs. dedicated ports

- The **multi-client port** (default `5001`) accepts any number of connections. Good for short-lived clients such as a one-off `meshcore-cli` session.
- Each **dedicated port** is one stable logical client with its own inbox that is kept while that client is disconnected, like a real companion does. When it reconnects, it receives the (bounded) backlog it missed. Use one dedicated port per long-lived app (MeshCore-HA, desktop app, phone app, bot) and never share one between apps.

## Quick start

Build (a project-local Go toolchain in `.tools/go` is used automatically, see [DEVELOPMENT.md](DEVELOPMENT.md)):

```sh
make build
```

Configure and run:

```sh
cp config.example.yaml config.yaml                  # set upstream.host and your ports
out/meshcore-mux --config config.yaml --probe   # verify the companion is reachable
out/meshcore-mux --config config.yaml
meshcore-cli -t 127.0.0.1 -p 5001 ver               # verify the mux
```

Then point MeshCore-HA at `5002`, your desktop app at `5003`, and so on. Flags work as in the original, for example `--upstream-host 192.168.1.50 --upstream-port 5000 --listen-dedicated-client-port 5002 --deduplicate-received-messages`; explicit flags override the file. All options are described in [CONFIGURATION.md](CONFIGURATION.md).

Container images for `linux/amd64` and `linux/arm64` (Raspberry Pi with a 64-bit OS) are published to `ghcr.io/fdyfox/meshcore-mux`: `latest` and version tags for releases, `test` for the current `main` branch. For Docker Compose and systemd, see [examples/](examples/) and [DEVELOPMENT.md](DEVELOPMENT.md). The listeners have no authentication: never expose them to the Internet.

## Persistence across restarts

By default, dedicated queues live in RAM. Enable `persistence` in the configuration file to keep them, together with the deduplication history, in a state file across restarts of the mux:

```yaml
persistence:
  enabled: true
  state_file: /var/lib/meshcore-mux/state.json
```

A graceful shutdown loses nothing; after a crash, at most the changes of the last second (`flush_interval`) are lost or repeated. The file contains received message text and is created with mode `0600`. Details in [CONFIGURATION.md](CONFIGURATION.md#persistence).

## Received-message deduplication

Clients are supposed to deduplicate retry copies of received messages, but many (including `meshcore-cli` and MeshCore-HA) don't. Setting `deduplicate_received_messages: true` (or `--deduplicate-received-messages`) makes the mux drop them before they reach clients. This is recommended, especially for Home Assistant automations and bots.

## Logging

`LOG_LEVEL` (`error`, `warn`, `info` (default), `debug`, `trace`) controls output to stderr. Every line has the form `event=<name> key=value ...`, and problems are always reported with their cause:

| Level | Meaning | Examples |
|---|---|---|
| `ERROR` | something is broken, a client was forcibly disconnected, or the companion connection was restarted | `command queue overflow`, `output queue overflow`, `inbox overflow`, `output write deadline`, companion timeout or protocol error (`event=upstream.epoch_ended`) |
| `WARN` | something did not work as intended, but operation continues | rejected command (`event=command.rejected` with result and reason), message loss in a full dedicated queue, client refused because the companion is not ready, companion unreachable or disconnected |
| `INFO` | significant state changes | connections, reconnects, replaced dedicated client, expired remote requests |
| `DEBUG` | individual commands, responses, pushes, and every wire frame as hex (CLI bodies are redacted) | |

Records that a misbehaving client could trigger in bulk (malformed frames, refused connections) appear at most once per second with `suppressed_since_last=<count>`.

## Project files

| File | Content |
|---|---|
| [CONFIGURATION.md](CONFIGURATION.md) | configuration file and flag reference |
| [ARCHITECTURE.md](ARCHITECTURE.md) | design, concurrency model, inbox and lease semantics |
| [LIMITATIONS.md](LIMITATIONS.md) | protocol and operational limitations |
| [DEVELOPMENT.md](DEVELOPMENT.md) | toolchain, build, tests, Docker, systemd, releases |
| [CHANGELOG.md](CHANGELOG.md) | changes and deviations from the original |
| [AGENTS.md](AGENTS.md) | rules for coding agents and contributors |

## License

MIT, see [LICENSE](LICENSE). Based on the original work by Michael F. Robbins.
