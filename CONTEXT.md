# CONTEXT.md - Developer Guidelines for ThySqueal

## Project Overview

ThySqueal is a SQL server with HTTP JSON API, built with Rust. It's a Cargo workspace with:

- `server/` - Server binary with Axum HTTP server; in-memory storage; SQL execution
- `client/` - CLI client with REPL; `--http -e "SQL"` for one-off queries

### Current Implementation Notes

- **Pluggable Engines**: Architecture supports multiple database protocols (MySQL, Redis, etc.) via the Engine trait
- **Protocol Trait**: Each engine implements Protocol trait for its own TCP server handler
- **Squeal IR**: Unified Internal Representation for all queries, decoupling surface syntax from execution
- **JSqueal**: Programmatic JSON-based query interface that maps directly to Squeal IR
- **SQL Parsing**: Uses Pest grammar (`engines/mysql/parser`) that maps SQL strings to Squeal IR
- **SQL Execution**: Modularized executor processing Squeal IR. Supports JOINs, Subqueries, Aggregations, GROUP BY, etc.
- **Materialized Views**: Pre-calculated views with automatic data refresh on mutations
- **Auto-Increment**: Support for `AUTO_INCREMENT` attribute and `SERIAL` data type
- **MySQL Protocol**: Native TCP support on port 13306
- **MySQL Authentication**: Native password auth with SHA1 challenge-response
- **Server Metrics**: Real `SHOW STATUS` with Uptime, Connections, Questions, etc.
- **Redis Protocol**: RESP-based TCP support on port 16379, routes through Squeal IR
- **Storage**: Hybrid in-memory storage with Sled-based WAL and snapshotting
- **Information Schema**: Virtual `information_schema` tables (tables, columns, indexes)

## Project Structure (Server)

```code
server/src/
├── main.rs              # Entry point
├── http.rs              # Axum HTTP handlers (SQL & JSqueal)
├── config.rs            # Configuration loading
├── engines/             # Pluggable protocol engines
│   ├── mod.rs           # Engine registry
│   ├── traits/          # Engine, Protocol, Config traits
│   │   ├── engine.rs    # Engine trait definition
│   │   ├── protocol.rs  # Protocol trait definition
│   │   ├── config.rs    # Config trait
│   │   ├── parser.rs    # Parser trait
│   │   └── registry.rs  # Engine registry
│   ├── mysql/           # MySQL protocol engine
│   │   ├── mysql_engine.rs
│   │   ├── protocol/    # MySQL TCP handler
│   │   ├── parser/      # SQL -> Squeal IR parser
│   │   ├── to_squeal/   # AST -> IR converter
│   │   └── ast/         # MySQL AST definitions
│   └── redis/           # Redis protocol engine
│       ├── redis_engine.rs
│       ├── protocol.rs  # Redis RESP handler
│       ├── to_squeal/   # RESP -> Squeal IR converter
│       └── connection/  # Command handlers
├── squeal/              # Internal Representation (IR)
│   ├── ir/              # IR definitions
│   │   ├── stmt.rs      # Statement IR (SQL + KV)
│   │   ├── expr.rs      # Expression IR
│   │   └── cond.rs      # Condition IR
│   └── exec/            # IR Executor
│       ├── executor.rs  # Main executor
│       ├── dispatch.rs  # Statement dispatcher
│       ├── kv/           # KV operations
│       └── dml/          # DML operations
└── storage/             # Storage Engine
    ├── database.rs       # Database state
    ├── table/            # Table, index, mutations
    └── value/            # Data types
```

## Build, Test, and Development Commands

### Workspace Commands

```bash
# Build all binaries
cargo build

# Build specific binary
cargo build -p thysqueal-server   # Server
cargo build -p thysqueal-cli      # Client

# Run server (HTTP on port 8888)
cargo run -p thysqueal-server

# Run server with custom config
cargo run -p thysqueal-server -- -c myconfig.yaml

# Run client
cargo run -p thysqueal-cli
```

### Testing

```bash
# Run all tests (78+ tests)
cargo test

# Run tests with output
cargo test -- --nocapture
```

### Linting and Formatting

```bash
# Run clippy for linting
cargo clippy -- -D warnings

# Format code
cargo fmt

# Run pre-commit hooks
pre-commit run --all-files
```

## Adding a New Engine

1. Create `engines/<name>/` module with:
   - `<name>_engine.rs` - implements Engine trait
   - `protocol.rs` - implements Protocol trait
   - `to_squeal/` - converter to Squeal IR (optional)

2. Register in `engines/mod.rs`:
   ```rust
   pub fn available_engines() -> Vec<Box<dyn Engine>> {
       vec![Box::new(MysqlEngine), Box::new(RedisEngine), Box::new(YourEngine)]
   }
   ```

3. Add port config in `config.rs` and `Registry::get_port()`

## Code Style Guidelines

- **Simplicity**: Keep logic focused and modular
- **Ownership**: Be careful with `DatabaseState` clones during mutation blocks
- **Documentation**: Update relevant Markdown files when changing architecture
- **Error Handling**: Use `SqlError` and `StorageError` for structured errors
- **Testing**: Add unit tests in `executor/tests/` and integration tests in `tests/`
