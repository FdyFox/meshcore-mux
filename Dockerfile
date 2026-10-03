# syntax=docker/dockerfile:1

# The build stage runs on the native build platform and cross-compiles, so
# multi-arch images build fast without CPU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY config.example.yaml ./
RUN go vet ./... && go test ./...
# Empty state directory owned by the runtime user, so a named volume mounted
# there inherits writable ownership for persistence.
RUN mkdir -p /out/state
ARG TARGETOS TARGETARCH
# Optional version override (CI sets it for test and release images).
ARG VERSION=""
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w ${VERSION:+-X github.com/FdyFox/meshcore-mux/internal/mux.Version=$VERSION}" \
      -o /out/meshcore-mux ./cmd/meshcore-mux

##########

FROM scratch
LABEL org.opencontainers.image.title="meshcore-mux" \
      org.opencontainers.image.description="Protocol-aware multiplexer that lets several apps share one TCP-connected MeshCore companion node" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /out/meshcore-mux /usr/local/bin/meshcore-mux
COPY LICENSE /usr/share/licenses/meshcore-mux/LICENSE
COPY --from=build --chown=10001:10001 /out/state /var/lib/meshcore-mux
USER 10001:10001
EXPOSE 5001/tcp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/meshcore-mux"]
CMD ["--help"]
