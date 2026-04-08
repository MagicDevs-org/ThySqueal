# MVP Architecture Suggestions

This document reflects the current architecture of **ThySqueal**, reaching beyond the initial MVP goals into a highly modular and robust SQL server.

---

## Current Architecture (Summary)

```code
Client (CLI/REPL)  →  POST /_query (SQL)  →  Parser (Pest)
                                              ↓
Client (App/API)   →  POST /_jsqueal (JSON) →  Squeal IR
                                              ↓
                                         SQL Executor
                                              ↓
                                         Storage Engine
```

---

## Architectural Pillars

### 1. Squeal IR (Internal Representation)

**Outcome**: Unified, strongly-typed query model.

- **Decoupling**: Separates the surface query language (SQL or JSON) from the execution logic.
- **Expressiveness**: Captures all SQL operations in a structured, serializable format.
- **Optimizability**: Provides a clean layer for future query optimizations.

### 2. Modular SQL Engine

**Outcome**: Clean separation of parsing, evaluation, and execution.

- **Parser**: Maps SQL strings to the internal `Squeal` IR.
- **JSqueal**: Direct JSON-to-IR mapping via Axum endpoint.
- **Evaluator**: Dedicated modules for column resolution, condition filtering, and expression evaluation.
- **Executor**: Processes `Squeal` IR via specialized command handlers (ddl, dml, select).

---

### 2. Robust Storage & Indexing

**Outcome**: High-performance in-memory storage with durable persistence.

- **Indexes**: Supports B-Tree and Hash indexes, including advanced features like JSON path, functional, and partial indexing.
- **Durability**: Synchronous Write-Ahead Logging (WAL) ensures data integrity across restarts.
- **Information Schema**: System metadata exposed via standard SQL queries.

---

### 3. ACID Transactions

**Outcome**: Atomicity and Isolation for complex operations.

- Uses `DatabaseState` snapshotting for transactional isolation.
- WAL logging for atomic `COMMIT` / `ROLLBACK` support.

---

## File Layout (Current)

```code
server/src/
├── main.rs          # Server Entry Point
├── config.rs        # Configuration Management
├── http.rs          # Axum HTTP API Handlers
├── sql/             # SQL Engine
│   ├── ast/         # Abstract Syntax Tree (Decomposed)
│   ├── squeal/      # Internal Representation (IR)
│   ├── eval/        # Runtime Evaluation (Modular)
│   ├── executor/    # Statement Execution (Modular)
│   │   ├── aggregate/    # Grouping/Aggregates
│   │   ├── dml/          # Insert/Update/Delete
│   │   └── select.rs     # SELECT logic
│   └── parser/      # Pest Parser (Modular)
└── storage/         # Storage Engine (Decoupled from AST)
    ├── mod.rs       # Database Entry Point
    ├── table/       # Modular Table Logic
    ├── row.rs       # Data Structures (Column, ForeignKey)
    ├── index.rs     # Indexing Logic
    ├── wal.rs       # WAL Management
    └── info_schema.rs # Metadata Tables
```

---

## Next Steps

| Feature | Status | Description |
| --- | --- | --- |
| SQL Dump/Restore | ✅ Done | Export/Import database state as .sql scripts |
| MySQL Protocol | ✅ Done | Support standard MySQL clients over TCP port 3306 |
| Parameterized Queries | ✅ Done | Prevention of SQL injection and query reuse |
| AUTO_INCREMENT | ✅ Done | Automated ID generation for integer columns |
| ALTER TABLE | ✅ Done | Non-destructive schema evolution |
| Materialized Views | ✅ Done | Pre-calculated query results with auto-refresh |
| Query Optimization | 🏗 Todo | Cost-based optimizer for join ordering |
