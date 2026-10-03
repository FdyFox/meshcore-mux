# Uses the project-local toolchain in .tools/go when present, otherwise `go` from PATH.
GO ?= $(if $(wildcard .tools/go/bin/go),$(CURDIR)/.tools/go/bin/go,go)
BIN := out/meshcore-mux

.PHONY: build test race vet fmt docker clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/meshcore-mux

test:
	$(GO) test ./...

race:
	$(GO) test -race -count=3 ./...

vet:
	$(GO) vet ./...

fmt:
	$(dir $(GO))gofmt -w cmd internal

docker:
	docker build -t meshcore-mux:dev .

clean:
	rm -rf out
