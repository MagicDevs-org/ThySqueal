# thy-squeal PRD - Product Requirements Document

## 1. Project Overview

### Project Name
**thy-squeal** - A lightweight SQL server with HTTP JSON API and Redis-like capabilities

### Project Type
Distributed in-memory database with SQL and HTTP interfaces

### Core Feature Summary
A MySQL-compatible SQL server with dual-protocol support (SQL over TCP + HTTP JSON API), featuring full-text search, dynamic caching, and Redis-like key-value operations. Includes an interactive JavaScript REPL client.

---

## 2. Architecture Overview

### Binary Distribution
| Binary | Port | Purpose |
|--------|------|---------|
| `thy-squeal` | 3306 (SQL), 9200 (HTTP) | Server daemon |
| `thy-squeal-client` | CLI | Interactive JS REPL + CLI tool |

---

## 3. Functional Requirements

### 3.1 SQL Server ( thy-squeal )

#### 3.1.1 SQL Dialect
- MySQL-compatible syntax (simplified subset)
- Support for: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE
- JOINs: INNER JOIN, LEFT JOIN (Pending)
- Aggregations: COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- ORDER BY and LIMIT/OFFSET
- Aliases for columns

---

## 7. File Structure

```
thy-squeal/                          # Cargo workspace
├── Cargo.toml                       # Workspace config
├── server/                          # Server crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                  # Server entry (Axum HTTP, /, /health, /_query)
│       ├── config.rs                # YAML config loading
│       ├── storage/                 # Modular storage engine
│       │   ├── mod.rs               # Database struct
│       │   ├── table.rs             # Table, Column, Row
│       │   ├── value.rs             # Value enum & impls
│       │   ├── types.rs             # DataType enum
│       │   └── error.rs             # StorageError
│       ├── sql/                     # SQL engine
│       │   ├── mod.rs               # SQL module entry
│       │   ├── ast.rs               # Abstract Syntax Tree
│       │   ├── eval.rs              # Expression/Condition evaluator
│       │   ├── error.rs             # SqlError enum
│       │   ├── parser/              # Pest-based parser (modular)
│       │   │   ├── mod.rs           # Parser entry
│       │   │   ├── expr.rs          # Expression parsing
│       │   │   ├── select.rs        # SELECT/GROUP BY/ORDER BY
│       │   │   ├── dml.rs           # INSERT/UPDATE/DELETE
│       │   │   └── ddl.rs           # CREATE/DROP
│       │   └── executor/            # SQL statement execution
│       │       ├── mod.rs           # Executor struct
│       │       ├── select.rs        # SELECT execution logic
│       │       ├── dml.rs           # INSERT/UPDATE/DELETE execution
│       │       └── ddl.rs           # CREATE/DROP execution
│       └── sql.pest                 # SQL grammar (Pest)
├── client/                          # Client crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                  # Client CLI (Clap)
│       ├── config.rs                # Client config (~/.thy-squeal/config.yaml)
│       ├── http.rs                  # HTTP client (POST /_query)
│       └── repl.rs                  # REPL (rustyline)
├── docs/
│   ├── PRD.md
│   ├── TODO.md
│   ├── MVP-ARCHITECTURE.md          # MVP architecture suggestions
│   └── features/
│       └── *.md
└── LICENSE, README.md
```

### Current Status (as of v0.1)
- [x] Workspace setup
- [x] Server binary with Axum HTTP on port 9200
- [x] Client binary with REPL
- [x] YAML config loading
- [x] GET /, GET /health, POST /_query endpoints
- [x] SQL grammar (`sql.pest`) — Modular Pest parser
- [x] In-memory storage: CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE
- [x] WHERE clause, ORDER BY, LIMIT support
- [x] Aggregations (COUNT, SUM, AVG, MIN, MAX)
- [x] GROUP BY and HAVING support
- [x] Column aliases
- [x] Structured Error Handling (SqlError)
- [x] Integration testing suite
- [x] REPL SQL execution (wired via HTTP)

---

## 8. Phases

### Phase 1: Foundation (v0.1)
- [x] Set up workspace with Cargo workspace
- [x] Server binary with Axum HTTP (port 9200)
- [x] Client binary with REPL
- [x] SQL parser using Pest (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, WHERE)
- [x] In-memory table storage
- [x] Integration tests

### Phase 2: HTTP API (v0.2)
- [x] HTTP JSON API (basic Axum server running)
- [x] POST /_query endpoint
- [x] GET /, GET /health
- [ ] GET /_stats
- [ ] CRUD endpoints for tables (REST)

### Phase 3: Advanced SQL (v0.3)
- [x] Wire Pest parser into executor (Completed)
- [x] WHERE clause filtering (Completed)
- [x] UPDATE, DELETE support (Completed)
- [x] Aggregations, GROUP BY, HAVING (Completed)
- [x] ORDER BY, LIMIT/OFFSET (Completed)
- [ ] JOINs
- [ ] Indexes
