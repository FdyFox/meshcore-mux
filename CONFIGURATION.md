# Configuration

Settings are resolved in this order, later sources winning:

1. built-in defaults,
2. the YAML file given with `--config FILE`,
3. command-line flags that are set explicitly.

Without `--config`, the binary behaves like the original and is configured with flags only. `--print-config` prints the effective configuration as YAML and exits, which is useful for debugging and as a starting point for your own file.

## Configuration file

Copy [config.example.yaml](config.example.yaml), which lists every option with its default and a comment, and edit it:

```sh
cp config.example.yaml config.yaml
vi config.yaml
meshcore-mux --config config.yaml --probe   # verify the companion is reachable
meshcore-mux --config config.yaml
```

- Every key is optional; missing keys keep their default. A minimal file only needs `upstream:`.
- Durations accept Go duration strings (`"20s"`, `"1m30s"`, `"500ms"`) or a plain number of seconds (`20`, `0.5`).
- Unknown keys are reported with their line number instead of being ignored, so typos cannot silently fall back to defaults.

### Reference

| Key | Flag | Default | Meaning |
|---|---|---|---|
| `upstream.host` | `--upstream-host` | (required) | companion hostname or IP |
| `upstream.port` | `--upstream-port` | (required) | companion TCP port, usually `5000` |
| `listen.host` | `--listen-host` | `127.0.0.1` | listener address; use `0.0.0.0` inside a container |
| `listen.multi_client_port` | `--listen-multi-client-port` | `5001` | shared port for any number of connections |
| `listen.dedicated_client_ports` | `--listen-dedicated-client-port` (repeatable) | none | one stable client identity per port |
| `offline_queue_size` | `--offline-queue-size` | `256` | entries per dedicated queue |
| `deduplicate_received_messages` | `--deduplicate-received-messages` | `false` | drop radio-retry duplicates of received text |
| `security.allow_private_key_export` | `--reject-private-key-export` | `true` | allow `EXPORT_PRIVATE_KEY` |
| `security.allow_private_key_import` | `--reject-private-key-import` | `true` | allow `IMPORT_PRIVATE_KEY` |
| `security.allow_factory_reset` | `--reject-factory-reset` | `true` | allow `FACTORY_RESET` |
| `persistence.enabled` | | `false` | keep dedicated queues across restarts, see [Persistence](#persistence) |
| `persistence.state_file` | | `/var/lib/meshcore-mux/state.json` | state file; its directory must be writable |
| `persistence.flush_interval` | | `1s` | how often changes are written at most |
| `timing.response_timeout` | `--response-timeout` | `20s` | maximum wait for a companion reply; also the idle limit while listing contacts |
| `timing.contacts_timeout` | `--contacts-timeout` | `30s` | total limit for a contacts listing; must not be shorter than `response_timeout` |
| `timing.poll_interval` | `--poll-interval` | `5s` | how often clients are reminded to check for messages |
| `advanced.command_limit` | | `16` | queued plus active commands per connection before it is disconnected |
| `advanced.command_age` | | `15s` | queued commands older than this are rejected with `BAD_STATE` |
| `advanced.virtual_sync_timeout` | | `30s` | how long a client sync may wait for an inbox result |
| `advanced.inbox_entries` | | `256` | per multi-client inbox before the connection is closed |
| `advanced.output_frames` | | `512` | pending writes per connection before it is closed |
| `advanced.connect_timeout` | | `5s` | DNS plus TCP connect to the companion |
| `advanced.frame_timeout` | | `5s` | maximum time to assemble one partially received frame |
| `advanced.write_timeout` | | `5s` | maximum time for one socket write |
| `advanced.startup_timeout` | | `15s` | startup synchronization with the companion |
| `advanced.signing_timeout` | | `30s` | inactivity limit of a signing operation |
| `advanced.radio_uncertainty_timeout` | | `60s` | radio quarantine after a send with unknown outcome |

`--maintenance` and `--allow-private-key-export` are accepted as no-ops for compatibility with older command lines.

## Endpoints and network access

There are two different endpoints:

- The **upstream endpoint** is the physical companion, such as `192.168.1.50:5000`. Only the mux connects to it.
- The **downstream endpoints** are the mux listener ports. All participating clients connect to these ports instead of the companion.

Inside a container, keep `listen.host: 0.0.0.0`; Docker's `ports` entries control which host interfaces can reach the listeners. A binding such as `127.0.0.1:5001:5001/tcp` is reachable only from the Docker host. The listeners have no authentication or encryption. Do not expose them to the Internet; use a firewall or VPN.

## Multi-client and dedicated ports

The multi-client port accepts multiple simultaneous connections. Each connection is a temporary client identity and does not receive messages distributed before it connected.

Each dedicated port is one stable logical client. Only one socket can use it at a time; a new connection replaces the old one, matching the firmware's TCP behavior. The internal listener port number is the client's identity, even if Docker maps it to a different host port. Do not share one dedicated port between several apps.

## Dedicated queues

Dedicated queues hold incoming messages only and live in RAM. Without [persistence](#persistence) they are lost when the process stops. When a queue is full, the mux follows firmware priority: it removes the oldest channel message to make room; if there is none, it discards the new arrival and logs a warning. An abandoned dedicated client never stalls other clients. Large queues can make initial catch-up slow because clients pull one item per sync request.

Messages remain on the companion until at least one connected client requests a sync. Once draining begins, each result is copied to all configured dedicated queues, including disconnected ones, and to live multi-client sessions. An item leaves a dedicated queue when the mux accepts it for socket output, so a connection failure immediately afterwards can still lose it.

## Persistence

With `persistence.enabled: true`, the mux saves the dedicated queues, the companion identity they belong to, and the deduplication history to `persistence.state_file`, and restores them on the next start. A dedicated client that was offline while the mux restarted still receives the messages it missed.

What it covers and what it does not:

- **Graceful shutdown** (SIGTERM, SIGINT, `docker stop`, `systemctl stop`) always writes the final state; nothing is lost.
- **Crash or power loss:** changes are written at most once per `flush_interval` (default `1s`). Messages fetched during the last interval can be missing, and messages delivered during it can be delivered again.
- **Messages still on the companion** are never at risk: the mux only fetches them when a client syncs, so they simply wait while the mux is down.
- **Multi-client connections** are not persisted; they never have a backlog.
- **Another companion:** if the companion's public key differs from the one in the file, the restored queues and history are discarded with a warning, exactly as on a live reconnect to a different node.
- **Changed configuration:** queues for ports that are no longer configured are dropped with a warning. If `offline_queue_size` was reduced, the usual overflow priority applies (oldest channel messages first).
- **Damaged file:** an unreadable file is renamed to `state.json.corrupt-<timestamp>` for inspection, and the mux starts with empty queues.

The file is written atomically (temporary file, `fsync`, rename), so a crash during a save never leaves a half-written file. It is JSON with hex-encoded native frames and is at most a few hundred kilobytes.

**Privacy:** the file contains received message text in plain form (the companion has already decrypted it). It is created with mode `0600`; keep its directory private as well.

The directory must be writable by the mux:

- **systemd:** the example unit sets `StateDirectory=meshcore-mux`, which provides `/var/lib/meshcore-mux`.
- **Docker:** mount a named volume at `/var/lib/meshcore-mux` (see [examples/compose.yaml](examples/compose.yaml)); the image prepares that directory for its non-root user, so `read_only: true` still works. For a bind mount, make the host directory writable for UID 10001.
- **Local runs:** point `state_file` to a writable path, for example `./state/state.json`.

Save failures are logged as errors, at most once per second.

## Received-message deduplication

`deduplicate_received_messages` discards retry copies of received text DMs and channel messages before they enter downstream queues. Its bounded history (1,024 identities) is shared by all clients, survives an upstream reconnect to the same companion identity, and is saved with [persistence](#persistence) when enabled. Discarded copies are logged at `DEBUG`. It is disabled by default because the firmware does not do this, but enabling it is recommended for most setups, especially for Home Assistant automations and bots.

## Sensitive commands

By default the mux behaves like a direct companion connection: private-key export, private-key import, and factory reset are available. Export replies go only to the requesting connection. Import and factory reset work only while exactly one client is connected. Each can be disabled independently; a disabled command is answered natively (`DISABLED` or `ERR`) and logged as a warning.

## Logging

`LOG_LEVEL` (`error`, `warn`, `info`, `debug`, `trace`) is an environment variable, not part of the file. See [README.md](README.md#logging) for what each level contains.
