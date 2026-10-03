# Architecture

`meshcore-mux` lets several existing MeshCore companion clients share one physical companion over its native TCP protocol. It is a protocol-aware command broker, not a second companion implementation:

- Client commands and firmware responses remain native binary payloads.
- One upstream connection is shared by many independent downstream sessions.
- Commands are serialized because ordinary responses such as `OK` and `ERR` contain no request or client identifier.
- Incoming messages are fetched once after a downstream sync request and copied into every multi-client inbox and every configured dedicated queue.
- Shared physical state (identity, contacts, channels, configuration, radio capacity) is deliberately not virtualized.

The design follows the [Crystal original](https://github.com/compumike/meshcore-tcp-mux) at version 1.3.1. Firmware protocol level 13 and newer is accepted. The mux requests app target 14 upstream and advertises at most protocol 14 downstream with the known 82-byte `DEVICE_INFO` record. Payloads are at most 176 bytes.

## Concurrency model

```text
 listener goroutines ──accepted──┐
                                 ▼
 client Endpoint ─(reader/writer)─► coordinator goroutine (Runtime) ◄─(reader/writer)─ upstream Endpoint
        ▲                               │        ▲
        └────────── write queue ◄───────┘  events│ actions
                                                 ▼
                                             Broker (no I/O)
```

- A single coordinator goroutine in `Runtime` receives every event through channels (`select`) and is the only caller of the `Broker`, which therefore needs no locks.
- Each socket has exactly one reader and one writer goroutine. The writer queue is bounded and `Enqueue` never blocks. A full queue counts as a write failure instead of accumulating unbounded bytes.
- The broker only produces `Action` values (`SendFrame`, `CloseSession`, `EndEpoch`, `Diagnostic`). The runtime executes them after each event, which keeps the state machine directly testable.

## Implementation map

| File | Responsibility |
|---|---|
| [`cmd/meshcore-mux/main.go`](cmd/meshcore-mux/main.go) | flags, merging defaults, config file and flags, signals, starting probe or runtime |
| [`runtime.go`](internal/mux/runtime.go) | listeners, upstream epochs, reconnect with backoff, startup fence, executing broker actions, poisoned drain, `Probe`, watchdog |
| [`transport.go`](internal/mux/transport.go) | `Endpoint`: socket ownership, framing, frame deadline, bounded writer queue |
| [`frame.go`](internal/mux/frame.go) | incremental decoder and encoder for the `<`/`>` envelope (direction byte, 16-bit little-endian length); rejects wrong markers, empty or oversized frames, truncated frames, and frames exceeding their assembly deadline |
| [`broker.go`](internal/mux/broker.go) | sole owner of command scheduling, response ownership, inbox pumping, push routing, leases, output budgets, and failure decisions |
| [`state.go`](internal/mux/state.go) | `Session` (command FIFO, virtual inbox, output budget, protocol target, flood scope, pending sync), `DedicatedClientSlot` (offline queue across reconnects), `ReceivedMessageDeduplicator` |
| [`protocol.go`](internal/mux/protocol.go) | command descriptors, payload validation, response grammars, V3-to-legacy inbox conversion, startup payloads |
| [`describe.go`](internal/mux/describe.go) | decoded log descriptions of commands, responses, and wire frames |
| [`leases.go`](internal/mux/leases.go) | `DmRing`, `RemoteLease`, `SigningLease`, `CompanionRadioState`: firmware state that outlives one immediate response |
| [`persistence.go`](internal/mux/persistence.go) | optional state file: snapshot, atomic write on a writer goroutine, restore with identity check |
| [`startup.go`](internal/mux/startup.go) | synchronization fence, protocol and identity checks, flood-scope reset |
| [`config.go`](internal/mux/config.go), [`configfile.go`](internal/mux/configfile.go) | limits, deadlines, permissions; YAML file; monotonic clock `Now()` |
| [`logger.go`](internal/mux/logger.go) | leveled logger controlled by `LOG_LEVEL` |

## Command and response flow

- A client endpoint decodes a complete command and reports it to the runtime.
- The runtime passes it to the broker, which validates it and queues it on the corresponding `Session`.
- Locally virtualized commands are answered immediately without involving the companion: `SYNC_NEXT_MESSAGE` from the session's own inbox, `SET_FLOOD_SCOPE_KEY`, and disabled sensitive commands.
- The broker schedules queue heads round-robin. At most one upstream transaction is active, including internal inbox pops and hidden flood-scope commands.
- The active `Transaction` records its owner and response grammar before its write reaches the upstream writer, so a response arriving before the write-completion event is still attributed correctly.
- Ordinary responses are validated against that grammar and sent only to the owner. Contacts remain owned through `END_OF_CONTACTS`; a stream cannot be interleaved with another command.
- Asynchronous pushes follow an explicit policy: shared observations are broadcast, remote results go only to their lease owner, DM confirmations are broadcast and release the matching ring slot, and `MSG_WAITING` prompts clients to sync without draining the inbox by itself.
- Epoch, session, and write IDs make late asynchronous completions harmless after a connection has been replaced.

Clients must still serialize ambiguous concurrent waits within their own TCP connection. The native protocol cannot tell two waiters in the same session which generic `OK` belongs to whom.

## Virtual inbox

The companion's `SYNC_NEXT_MESSAGE` removes an item from one physical queue. Forwarding every client's sync would split messages between clients, so the broker is the only consumer of the upstream inbox.

- Admission, `MSG_WAITING`, and fallback polling only emit coalesced hints to clients and never pop the physical inbox. Only a client sync against an empty local queue authorizes a drain-to-empty cycle.
- A fetched message is stored as immutable native bytes and fanned out to every configured dedicated slot (attached or not) and every live multi-client session.
- A client sync consumes one item from that session's queue only. `NO_MORE_MESSAGES` is returned only after a qualifying upstream empty check, so a stale empty observation cannot overtake an in-flight message.
- An empty-to-nonempty transition emits one coalesced `MSG_WAITING`. Hints are neither counts nor delivery acknowledgements.
- A multi-client inbox overflow disconnects only that session. A dedicated queue follows firmware priority: when full, it evicts the oldest channel message, or discards the new message if no channel message exists. Neither outcome blocks other recipients.
- With no sessions, the broker stops draining the companion. If the last client leaves during a pop, at most one unfanned item is retained and reused only when the next epoch has the same companion identity.

Multi-client sessions provide live fan-out, not history. Dedicated clients use their port as stable identity and receive unconsumed items after reconnecting. This is still not exactly-once delivery: queues are bounded, an item counts as consumed once broker output accepts it, and by default queues live in RAM only (see [Persistence](#persistence)).

## Stateful operations

- Remote commands reserve `RemoteLease` beyond their immediate `SENT` reply, because the later radio result carries no client identity.
- Remote leases and DM acknowledgement-ring positions survive replacement of the upstream connection when startup identifies the same companion (public key). Their old owners are removed, so late results are never attributed to a new session. If TCP fails before `SENT`/`ERR`, execution is unknown; new radio work is quarantined for a bounded interval while ordinary queries remain available. A changed key clears this state.
- Signing reserves `SigningLease` across start, data chunks, and finish so no other client can corrupt the shared signing operation.
- Plain direct messages reserve one of the firmware's eight acknowledgement slots in `DmRing`. `SEND_CONFIRMED` frames are forwarded unchanged and matched by their native four-byte token.
- Protocol target and temporary flood scope are per session. `protocol.go` downgrades V3 inbox messages for legacy clients, and the broker wraps scoped sends in acknowledged setup and restore commands.
- Channel `OK` and direct-message `SENT` mean firmware acceptance, not radio delivery. The mux never retries a radio send, changes a timestamp, creates an outgoing-message echo, or fabricates a confirmation.

Resource conflicts return native `ERR(BAD_STATE)` in the client's FIFO order and are logged as warnings with their cause. They do not block unrelated local queries or inbox work.

## Failure and operational boundaries

- Malformed or slow clients are disconnected individually (logged as errors naming the limit).
- A malformed upstream frame, unexpected response, upstream write failure, or response timeout ends the whole epoch. All sessions disconnect and no possibly executed command is replayed.
- If a response is still outstanding after a timeout ("response debt"), the runtime keeps the old connection open for up to one `response_timeout` and discards replies arriving there (poisoned drain). If that fails, it waits one more response horizon before reconnecting, so a late handler cannot write into the new connection.
- The runtime reconnects with bounded exponential backoff (0.5 s to 30 s, with jitter) and repeats the startup fence before admitting clients. Clients connecting meanwhile are refused and logged as warnings.
- Reboot uses normal scheduling and ends when the companion disconnects. Factory reset and private-key import are allowed by default, require exactly one client and no outstanding radio or signing lease, and can be disabled independently.

The mux must be the companion's **only command producer across TCP, BLE, and USB**. Another producer can inject untagged responses and make ownership unknowable.

## Persistence

Optional (`persistence.enabled`). The persistable state is the dedicated queues, the companion key they belong to, and the deduplication history. Multi-client inboxes and radio leases are not persisted; leases describe firmware work whose state after a restart is unknown, so starting without them is the conservative choice.

- `DedicatedClientSlot.Changes` and the deduplicator's change counter form a cheap generation number. After every coordinator iteration of a running epoch, `maybePersist` compares it with the last saved generation and, at most once per `flush_interval`, builds an immutable snapshot. Queued payloads are never mutated, so the snapshot shares their bytes safely.
- The snapshot goes to a writer goroutine through a one-slot channel that keeps only the newest pending snapshot. Writing (temporary file, `fsync`, rename, directory `fsync`) therefore never blocks the coordinator, even on slow storage.
- On shutdown the writer is stopped and the final state is written synchronously after all clients are closed.
- On start, `restoreState` loads the file before the listeners open, sets the remembered companion key, and enqueues valid inbox frames through the normal overflow policy. The first startup fence then compares the key: a different companion clears the restored state exactly like an in-process reconnect to another node would.

## Startup fence

Native TCP retains up to four outgoing frames across client replacement. `Startup` therefore sends five `APP_START` commands and one `DEVICE_QUERY`. Only five consecutive `SELF_INFO` replies followed by `DEVICE_INFO` prove that the new handshake has been reached and no stale replies remain. The flood scope is then reset to the default, and only then does the runtime admit clients.

## Watchdog

The `Watchdog` is a process-wide last resort. `main.go` arms it before the runtime starts; `runEpoch` resets it after every coordinator iteration (every 100 ms, even without traffic). It is deliberately not reset during connecting, startup, or backoff: if the process cannot reach a running epoch for five minutes, it exits with status 142 so a supervisor (systemd, Docker) restarts it. The original uses `SIGALRM`; this port uses a Go timer.

## Verification

The tests cover framing boundaries, command validation, scheduling and ownership, inbox fan-out, dedicated queues, leases, scope wrapping, timeouts, log levels, and the configuration file. `runtime_test.go` runs the real runtime against a simulated companion over loopback TCP with several real client sockets. See [DEVELOPMENT.md](DEVELOPMENT.md).
