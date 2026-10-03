# Changelog

---

## Unreleased

- The container image listens on `0.0.0.0` by default through the new `MESHCORE_MUX_LISTEN_HOST` environment variable, so Docker setups no longer need `listen.host` or `--listen-host`. Native installs keep `127.0.0.1`. The configuration file and explicit flags still take precedence; `config.example.yaml` leaves `listen.host` unset for this reason.
- README: the Docker Compose quick-start now uses a mounted `config.yaml` with persistence enabled and `restart: unless-stopped`; the flags-only setup is shown as an alternative. `examples/compose.yaml` matches it.
- CI: update GitHub Actions to their current major versions (`actions/checkout@v7`, `actions/setup-go@v7`, `docker/metadata-action@v6`, `docker/setup-buildx-action@v4`, `docker/login-action@v4`, `docker/build-push-action@v7`), which run on Node.js 24 instead of the deprecated Node.js 20.

---

## 0.5.0

Initial release: a Go port of [compumike/meshcore-tcp-mux](https://github.com/compumike/meshcore-tcp-mux) 1.3.1. Protocol behavior, limits, and command-line flags match the original.

Additions:

- YAML configuration file (`--config`), with defaults for missing keys, errors for unknown keys, and explicit flags overriding the file. `--print-config` prints the effective configuration. See `config.example.yaml`.
- Logging policy: problems are reported at `WARN`/`ERROR` with their cause instead of only at `DEBUG`.
  - Forced disconnects (`command queue overflow`, `output queue overflow`, `inbox overflow`, `output write deadline`, writer failures) are errors naming the limit.
  - Rejected commands are warnings (`event=command.rejected`) with command name, result, and a human-readable reason.
  - Companion timeouts and protocol errors that end an upstream epoch are errors.
  - Refused clients while the companion is not ready, and undelivered dedicated messages at shutdown, are warnings.
  - Reconnect scheduling and dedicated-client attach/detach are info.
- Optional persistence (`persistence.enabled`): dedicated-client queues, their companion identity, and the deduplication history are saved to a state file and restored on start. Writes are atomic, rate-limited by `persistence.flush_interval`, done off the coordinator goroutine, and always completed on graceful shutdown. A different companion identity, unconfigured ports, invalid entries, and corrupt files are handled with warnings. The Docker image and the systemd example provide a writable `/var/lib/meshcore-mux`.
- Multi-arch container images (`linux/amd64`, `linux/arm64`) on `ghcr.io/fdyfox/meshcore-mux`, built by a GitHub Actions workflow that tests every pull request and push: `test` and `sha-<commit>` for pushes to `main` (with the commit in `--version`), `<version>`, `<major>.<minor>`, and `latest` for published releases. Releases are checked against `Version` and this changelog.

Deliberate deviations from the original:

- Renamed to `meshcore-mux` (binary, Go module, Docker image, systemd unit, and the application name announced in `APP_START`) so it cannot be confused with the original. Command-line flags are unchanged.
- Dedicated-queue overflow (message eviction or discard) is logged as a rate-limited `WARN`; the original lowered it to `DEBUG` in 1.1.4.
- The process watchdog uses a Go timer instead of `SIGALRM` (same five-minute limit and exit status 142).
- Log lines use a compact `timestamp LEVEL event=...` format instead of Crystal's `Log` format.
- The container image is based on `scratch` instead of Alpine.
- Durable dedicated-queue storage, listed as "not planned" in the original's roadmap, is available as the opt-in persistence above.
- Versioning starts at 0.5.0 independently of the original's 1.x versions.
