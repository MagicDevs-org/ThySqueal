.PHONY: help build build-release run run-server run-client test lint fmt clean deps

help:
	@echo "thy-squeal Makefile"
	@echo ""
	@echo "Available commands:"
	@echo "  make build          - Build all binaries (debug)"
	@echo "  make build-release - Build all binaries (release)"
	@echo "  make run-server    - Run the server"
	@echo "  make run-client    - Run the client"
	@echo "  make test          - Run all tests"
	@echo "  make lint          - Run clippy lints"
	@echo "  make fmt           - Format code"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make deps          - Show dependencies"
	@echo ""
	@echo "Server runs on http://localhost:9200"

build:
	cargo build

build-release:
	cargo build --release

run-server:
	cargo run -p thy-squeal

run-client:
	cargo run -p thy-squeal-client

test:
	cargo test

test-watch:
	cargo watch -x test

lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

check:
	cargo check

clean:
	cargo clean

deps:
	cargo tree

update-deps:
	cargo update

doc:
	cargo doc --no-deps

# Run server in background
server-bg:
	@echo "Starting server in background..."
	@cargo run -p thy-squeal &

# Kill server process
server-stop:
	@pkill -f "thy-squeal" || true

# Quick rebuild and run
rebuild:
	cargo build -p thy-squeal && cargo run -p thy-squeal
