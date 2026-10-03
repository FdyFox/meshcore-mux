# Development

## Toolchain

The project uses a project-local Go toolchain in `.tools/go` (ignored by git). The Makefile picks it up automatically; otherwise it uses `go` from `PATH`. To install or update it:

```sh
V=$(curl -fsSL 'https://go.dev/VERSION?m=text' | head -1)
rm -rf .tools/go && mkdir -p .tools
curl -fsSL "https://go.dev/dl/$V.linux-amd64.tar.gz" | tar -xz -C .tools
.tools/go/bin/go version
```

Use `linux-arm64` on 64-bit ARM hosts. With [direnv](https://direnv.net/), `direnv allow` puts the toolchain on `PATH` (see `.envrc`); without it, `export PATH=$PWD/.tools/go/bin:$PATH` works too.

## Build and test

```sh
make build   # static binary at out/meshcore-mux (CGO_ENABLED=0)
make test    # unit and integration tests
make race    # tests with the race detector, three runs
make vet
make fmt
```

The only external dependency is `gopkg.in/yaml.v3`. Cross-compiling needs no extra tooling, for example `GOOS=linux GOARCH=arm64 make build` for a Raspberry Pi with a 64-bit OS.

## Run locally

```sh
cp config.example.yaml config.yaml   # set upstream.host
out/meshcore-mux --config config.yaml --probe
LOG_LEVEL=debug out/meshcore-mux --config config.yaml
```

`--probe` performs startup synchronization, prints public firmware identification, and exits without starting a listener. Stop a running mux before probing the companion directly. Then verify with a real client:

```sh
meshcore-cli -t 127.0.0.1 -p 5001 ver
meshcore-cli -t 127.0.0.1 -p 5001 list
```

## Docker

```sh
make docker                                   # builds meshcore-mux:dev for the local architecture
docker run --rm meshcore-mux:dev --version    # quick check of the local image
```

To run the local image with Docker Compose, use [examples/compose.yaml](examples/compose.yaml) with `image: meshcore-mux:dev` (or `build: ..`) instead of the published image.

The multi-stage [Dockerfile](Dockerfile) runs `go vet` and the tests, builds a static binary for the target platform (`TARGETOS`/`TARGETARCH`, so `docker buildx build --platform linux/amd64,linux/arm64` works), and copies only the binary and license into a `scratch` image running as UID 10001. [.dockerignore](.dockerignore) allowlists the build context so the local toolchain, binaries, and configuration files are never sent to the builder. The image needs only the configuration file, plus a volume at `/var/lib/meshcore-mux` when persistence is enabled; it can run with `read_only: true` and `cap_drop: ["ALL"]`.

## systemd

[examples/meshcore-mux.service](examples/meshcore-mux.service) runs the binary from `/usr/local/bin` with a hardened sandbox and reads `/etc/meshcore-mux/config.yaml`:

```sh
sudo install -m 0755 out/meshcore-mux /usr/local/bin/
sudo install -D -m 0644 config.yaml /etc/meshcore-mux/config.yaml
sudo install -m 0644 examples/meshcore-mux.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now meshcore-mux
journalctl -u meshcore-mux -f
```

## Tests

Tests live next to the code in `internal/mux`:

| File | Covers |
|---|---|
| `codec_test.go` | framing boundaries and errors, command validation, inbox downgrade, `DEVICE_INFO` capping, deduplication, describe functions never panicking on truncated frames |
| `broker_test.go` | response ownership and FIFO, inbox fan-out, dedicated queues and replacement, scope wrapping, remote leases, timeouts and response debt, log levels of problems |
| `configfile_test.go` | example file, partial files, typos, durations, round trip |
| `persistence_test.go` | state file format and permissions, restore filtering, identity change, corrupt files, and a real restart of the runtime that keeps a dedicated client's message |
| `runtime_test.go` | the real runtime against a simulated companion over loopback TCP with multi-client and dedicated clients |

Broker tests use a harness that injects monotonic time and acknowledges writes immediately; no test contacts a physical radio. Real-client smoke tests with `meshcore_py`/`meshcore-cli`, as in the original, are not ported yet.

## Continuous integration

[.github/workflows/container.yml](.github/workflows/container.yml) runs on every pull request, every push to `main`, and every published release:

1. **Test:** `gofmt` check, `go vet`, and `go test -race -count=3`.
2. **Image:** multi-arch build (`linux/amd64`, `linux/arm64`) with the Dockerfile, pushed to the GitHub Container Registry, followed by a smoke test that runs `--version` and `--help` on the published image.

| Trigger | Published tags | `--version` reports |
|---|---|---|
| pull request | nothing (build only) | |
| push to `main` (or manual run) | `test`, `sha-<commit>` | `0.5.0-test.<commit>` |
| published release `v0.5.0` | `0.5.0`, `0.5`, `latest` | `0.5.0` |
| published pre-release `v0.6.0-rc.1` | `0.6.0-rc.1` only | `0.6.0-rc.1` |

The image is `ghcr.io/fdyfox/meshcore-mux`. Publishing uses the workflow's `GITHUB_TOKEN`; no secrets are needed. GHCR creates the package as private on the first push; to allow anonymous pulls, set its visibility to public under the package settings on GitHub. The Docker build cross-compiles on the runner's native platform (`--platform=$BUILDPLATFORM`), so the ARM image needs no CPU emulation, and layers are cached in the GitHub Actions cache.

## Releases

1. Set `Version` in `internal/mux/config.go` to the new version (for example `0.6.0`) and move the "Unreleased" entries in [CHANGELOG.md](CHANGELOG.md) under a `## 0.6.0` heading.
2. Run `make fmt vet race build`, commit, and push to `main`. Check the resulting `:test` image.
3. Create a GitHub release with the tag `v0.6.0` and publish it. The workflow refuses to publish if the tag does not match `Version` or the changelog section is missing.

Do not move an already published version tag; use a new version for corrections. Mark release candidates as pre-releases so `latest` keeps pointing at the last stable version.
