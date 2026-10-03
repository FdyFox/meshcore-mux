# meshcore-mux (Go)

Guidance for coding agents and contributors. Read [ARCHITECTURE.md](ARCHITECTURE.md) before making larger changes.

## Development environment

### Go toolchain

The Go toolchain lives project-locally in `.tools/go` (not committed, see `.gitignore`). The Makefile uses it automatically when present and falls back to `go` from `PATH` otherwise. Prefer Makefile targets:

    make build
    make test
    make race
    make vet
    make fmt

For direct Go commands, use the local toolchain, either through direnv (`direnv exec . go ...`, see `.envrc`) or explicitly:

    .tools/go/bin/go <arguments>

Do not download another Go version or install anything system-wide. If `.tools/go` is missing, ask the user or follow [DEVELOPMENT.md](DEVELOPMENT.md). Run `gofmt` only on `cmd` and `internal`, never on `.tools`.

### Dependencies

The only external dependency is `gopkg.in/yaml.v3` for the configuration file. Do not add others without asking first. The daemon must stay small, statically linked (`CGO_ENABLED=0`), and low-maintenance.

## Continuous integration

`.github/workflows/container.yml` runs the same checks as the definition of done below and publishes images; see [DEVELOPMENT.md](DEVELOPMENT.md#continuous-integration). Keep `GO_VERSION` there and the `golang` image tag in the Dockerfile in sync. Releases are cut only by publishing a GitHub release whose tag is `v` plus `Version` from `internal/mux/config.go`.

## Definition of done

A change is complete only when:

- `make fmt` changes nothing and `make vet` is clean,
- `make race` passes (tests with the race detector, run several times),
- new protocol or state logic has a test in `internal/mux/*_test.go`,
- affected documentation (README, CONFIGURATION, ARCHITECTURE, LIMITATIONS) is updated,
- an entry exists under "Unreleased" in [CHANGELOG.md](CHANGELOG.md).

## Project structure and Go style

- `cmd/meshcore-mux/main.go` stays thin: parse flags, merge configuration, start `Runtime`. No protocol logic there.
- The whole implementation lives in package `internal/mux`.
- **The `Broker` performs no I/O.** It receives events with injected monotonic time (`time.Duration` since process start, see `Now()`) and returns `Action` values. Only the coordinator goroutine in `Runtime` calls it, so it needs no mutexes. Do not add goroutines, locks, `time.Now()`, or socket access to it.
- Socket I/O belongs in `transport.go` (one reader and one writer goroutine per socket with a bounded queue). Lifecycle, reconnects, and executing actions belong in `runtime.go`.
- Do not range over maps in the broker where the order becomes observable (broadcasts, delivery). Use `liveSessions()`/`sessionIDs` or `slotOrder` so behavior stays deterministic and testable.
- Return errors as `error`, not via `panic`. A `panic` is reserved for programming errors; the runtime recovers it and ends the epoch.
- Copy buffers (`dup`) that cross goroutine or ownership boundaries. A payload returned by the decoder belongs to its receiver.

## Comments and test readability

- Every type and nontrivial function gets a Go doc comment explaining its purpose, responsibility, and contract.
- Explain what code does and why, especially state transitions, response ownership, ordering, and failure handling. Do not assume the reader knows the wire protocol.
- Never leave protocol numbers unexplained. Use the named constants in `protocol.go` (`CmdSyncNextMessage`, `RespOk`, `PushMsgWaiting`, ...) or comment the value, for example:

  ```go
  case PushMsgWaiting: // MSG_WAITING: the companion has inbox data to drain.
  ```

- Explain wire-layout checks: which offsets and lengths count, whether the three-byte TCP envelope is included, byte order (little-endian), reserved fields, bit masks, and optional or version-dependent forms. Verify unfamiliar meanings against the firmware instead of guessing.
- Comment hard-coded byte fixtures in tests: command or response, opcode, important fields, and for deliberately malformed fixtures, why they are invalid.
- Walk through complex tests as conversations: who sends what, who owns the operation, which responses are visible or hidden, and what each assertion proves.
- Keep comments accurate when changing code; do not replace explanations with opaque constants.

## Logging

Problems must be visible to operators, with a reason. Follow this classification (details in [README.md](README.md#logging)):

| Category | When |
|---|---|
| `CatError` / `LevelError` | a client was forcibly disconnected, or the companion connection ended because of an error or timeout |
| `CatWarn` / `LevelWarn` | operation continues but something did not work as intended: rejected command, message loss, refused client |
| `CatInfo` / `LevelInfo` | significant state changes |
| `CatNormal`, `CatDebug` | individual commands, responses, pushes, payloads |

- Every record starts with `event=<name>` and uses `key=value` fields. Quote strings containing spaces with `strconv.Quote` or `%q`.
- Rejected commands go through `Broker.reject` or `logRejection` with a human-readable `detail` that explains the cause.
- Forced disconnects go through `Broker.remove(..., CatError)` with a reason naming the limit that was hit.
- Events a misbehaving client can trigger in bulk are rate-limited (`logLimiter`, `rateLimited`) and count `suppressed_since_last`.
- Log one cause only once at a high level.
- Guard expensive debug output (wire dumps) with `Log.Enabled(LevelDebug)`. CLI bodies (`RUN_CLI_COMMAND`/`CLI_REPLY`) stay redacted.

## Configuration

A new option always needs all of the following:

1. a field in `Config` with its default in `DefaultConfig()` and, if needed, a check in `Validate()`,
2. a field in `fileConfig` (`configfile.go`) with its `from`/`to` mapping,
3. a commented entry in [config.example.yaml](config.example.yaml) showing the default,
4. a row in [CONFIGURATION.md](CONFIGURATION.md),
5. optionally a command-line flag in `main.go`, applied only when explicitly set (`fs.Visit`).

Durations use the `Duration` type in the file.

The persisted state format (`persistence.go`) is versioned by `stateFileVersion`. A change to its layout must bump the version, and the loader must keep reading the previous version or document that old files are discarded. Existing flags of the original must stay compatible.

## Compatibility with the original

This project is a port of [compumike/meshcore-tcp-mux](https://github.com/compumike/meshcore-tcp-mux). Protocol behavior, limits, and CLI flags should match the original. Document deliberate deviations in [CHANGELOG.md](CHANGELOG.md) and, where relevant, in [LIMITATIONS.md](LIMITATIONS.md).

## Portable, non-identifying examples

- Do not put personal contact or channel names, private addresses, real keys, or user-specific paths in code, tests, or docs.
- Use clearly synthetic fixtures and example addresses such as `192.168.1.50`.

## Language and Markdown

- Code, identifiers, comments, log messages, and documentation (`*.md`) are in English.
- Do not add hard line breaks within paragraphs; editors use soft wrapping.
