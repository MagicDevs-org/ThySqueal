# ThySqueal Implementation TODO

## Phase 1: Foundation (v0.1) - ✅ COMPLETE

- [x] Project workspace setup (Cargo)
- [x] Basic in-memory storage (Database, Table, Row, Value)
- [x] Axum HTTP server with `/` and `/health`
- [x] Simple SQL parser (string-based)
- [x] Basic execution logic (CREATE, INSERT, SELECT)
- [x] Client CLI with REPL

## Phase 2: HTTP & Persistence (v0.2) - ✅ COMPLETE

- [x] Move to Pest-based parser for robust SQL
- [x] Map SQL errors to HTTP responses
- [x] Implement JSON API `POST /_query`
- [x] Implement Sled-based persistence (Snapshots)
- [x] Periodic and DML-triggered background saving

## Phase 3: Advanced SQL (v0.3) - ✅ COMPLETE

- [x] Implement WHERE clause with complex logic
- [x] Implement ORDER BY and LIMIT/OFFSET
- [x] Implement DISTINCT
- [x] Implement Aggregations (COUNT, SUM, etc.)
- [x] Implement GROUP BY and HAVING
- [x] Implement INNER and LEFT JOIN
- [x] Implement Correlated Subqueries
- [x] Implement EXPLAIN plan
- [x] Full-Text Search (Tantivy integration)
- [x] B-Tree Indexes (Range & Equality)
- [x] Hash Indexes (O(1) equality lookups)
- [x] Composite Indexes (Multi-column)
- [x] JSON Path Indexes (Indexing nested fields)
- [x] Functional Indexes (Expression-based indexing)
- [x] Partial Indexes (Conditional indexing)
- [x] Unique Constraints / Indexes

## Phase 4: ACID & Protocol (v0.4) - ✅ COMPLETE

- [x] Transactions (BEGIN, COMMIT, ROLLBACK)
- [x] Write-Ahead Logging (WAL) for durability
- [x] Information Schema (tables, columns metadata)
- [x] SQL Dump/Restore (.sql script export)
- [x] MySQL Protocol Compatibility (TCP 3306)
- [x] Parameterized Queries (Prepared Statements)

## Code Quality & Refactoring - ✅ COMPLETE

- [x] Decompose `eval.rs` (Expression vs Condition logic)
- [x] Decompose `executor/aggregate.rs` (Grouping vs Functions)
- [x] Decompose `parser/expr.rs` (Literals vs Logic)
- [x] Decompose `executor/dml.rs` (Insert/Update/Delete modules)
- [x] Move WAL recovery logic to `storage/wal.rs`
- [x] Decompose `storage/table.rs` (Index and Mutation logic)
- [x] Modularize test suite (`tests/` and `executor/tests/` directories)

## Phase 5: Compatibility & Ecosystem (v0.5) - ✅ COMPLETE

- [x] **ALTER TABLE**: Support for `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `RENAME TABLE`
- [x] **Advanced Schema Evolution**: Type changes and constraint modifications
- [x] **Constraints**: Proper `PRIMARY KEY` and `FOREIGN KEY` (Referential Integrity)
- [x] **AUTO_INCREMENT / SERIAL**: Automated ID generation for integer columns
- [x] **Standard SQL Functions**: `CONCAT`, `SUBSTRING`, `COALESCE`, `NOW()`, `DATE_FORMAT`, `CAST(x AS type)`
- [x] **CTEs (WITH clause)**: Common Table Expressions for complex query readability
- [x] **JSqueal**: JSON-based query language (direct IR mapping, bypassing Pest parser)
- [x] **Information Schema Expansion**: `statistics`, `key_column_usage`, `schemata` tables
- [x] Secondary Index optimization (using index only if selective)
- [x] Materialized Views
- [x] User Authentication & RBAC

## Phase 6: Key-Value Storage (v0.6) - ✅ COMPLETE

- [x] **Redis Protocol Compatibility**: Support for RESP protocol on port 6379
- [x] **Core Commands**: GET, SET, DEL, EXISTS, EXPIRE, TTL, KEYS
- [x] **Data Structures**: Hash (HSET/HGET/HDEL/HGETALL), Lists (LPUSH/RPUSH/LRANGE/LPOP/RPOP/LLEN), Sets (SADD/SREM/SMEMBERS/SISMEMBER), Sorted Sets (ZADD/ZRANGE/ZRANGEBYSCORE/ZREM)
- [x] **Streams (XADD, XREAD, etc.)**: XADD, XRANGE, XLEN
- [x] **Persistence**: RDB-style snapshots and AOF (Append Only File) integration with existing WAL
- [x] **Pub/Sub**: Basic message queuing and notification system
- [x] **SQL Integration**: Querying Key-Value data via SQL virtual tables
- [x] **Squeal IR Integration**: Redis commands now route through Squeal IR for unified execution

## Phase 7: MySQL Compatibility Improvements (v0.7) - ✅ COMPLETE

- [x] **System Variables**: Support for `@@version`, `@@max_allowed_packet`, etc.
- [x] **Extended Protocol**: Support for `COM_FIELD_LIST`, `COM_STATISTICS`, and better multi-result handling
- [x] **MySQL Error Codes**: Map `SqlError` to exact MySQL numeric error codes
- [x] **Advanced Functions**: `IFNULL`, `IF`, `DATEDIFF`, `DATE_FORMAT`, `MD5`, `SHA2`
- [x] **Session Variables**: Support for `SET @var = val` and `SELECT @var`

## Phase 8: Advanced SQL Capabilities (v0.8) - ✅ COMPLETE

- [x] **Window Functions**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTILE()`, `PARTITION BY`
- [x] **Set Operations**: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- [x] **Recursive CTEs**: Support for `WITH RECURSIVE` for hierarchical data
- [x] **Advanced Aggregations**: `GROUP_CONCAT`, `JSON_ARRAYAGG`, `JSON_OBJECTAGG`
- [x] **Common Table Expressions (CTEs) Expansion**: Support for multiple CTEs in a single query

## Pluggable Engine Architecture - ✅ COMPLETE

- [x] **Protocol Trait**: Each engine implements `Protocol` for TCP server handling
- [x] **Engine Trait**: Unified interface for all protocol engines
- [x] **Registry**: Dynamic engine discovery and spawning
- [x] **MySQL Engine**: Full implementation with Squeal IR
- [x] **Redis Engine**: Full implementation with Squeal IR

## Engine Implementation Roadmap - 🏗 IN PROGRESS

### PostgreSQL Protocol Engine - 🏗 IN PROGRESS

```
engines/postgres/
├── postgres_engine.rs    # Engine trait impl
├── protocol.rs       # PostgreSQL wire protocol (port 5432)
├── to_squeal/      # PG AST -> Squeal IR
└── parser/         # PostgreSQL parser
```

- [ ] **Wire Protocol**: Implement PG protocol handler (Startup, Query, Parse, Bind, Execute, etc.)
- [ ] **PostgreSQL Dialect**: Support PG-specific syntax ($$ quoting, RETURNING, etc.)
- [ ] **Types**: PG-specific type handling (UUID, JSONB, ARRAY, etc.)
- [ ] **Prepared Statements**: Extended protocol support
- [ ] **Copy Protocol**: Binary copy in/out

### MongoDB Protocol Engine - 📋 TODO

```
engines/mongo/
├── mongo_engine.rs   # Engine trait impl
├── protocol.rs      # MongoDB wire protocol (port 27017)
├── to_squeal/      # BSON/MongoDB query -> Squeal IR
└── connection/     # Command handlers
```

- [ ] **Wire Protocol**: MongoDB wire protocol (OP_MSG, OP_QUERY)
- [ ] **BSON Support**: Convert BSON documents to Squeal IR
- [ ] **Mongo Queries**: Convert find/aggregate to Squeal IR
- [ ] **CRUD Operations**: Insert, Update, Delete, Find

### MSSQL Protocol Engine - 📋 TODO

```
engines/mssql/
├── mssql_engine.rs   # Engine trait impl
├── protocol.rs      # TDS protocol (port 1433)
└── to_squeal/     # T-SQL -> Squeal IR
```

- [ ] **TDS Protocol**: Tabular Data Stream protocol
- [ ] **T-SQL Support**: Microsoft SQL Server dialect
- [ ] **Parameterized Queries**: TDS parameter handling

### Oracle Protocol Engine - 📋 TODO

```
engines/oracle/
├── oracle_engine.rs # Engine trait impl
├── protocol.rs     # Oracle wire protocol (port 1521)
└── to_squeal/    # PL/SQL -> Squeal IR
```

- [ ] **Oracle Wire Protocol**: OCI/TNS protocol
- [ ] **PL/SQL Subset**: Basic stored procedure support

### Elasticsearch Protocol Engine - 📋 TODO

```
engines/elastic/
├── elastic_engine.rs # Engine trait impl
├── protocol.rs      # HTTP/REST (port 9200)
└── to_squeal/     # Elasticsearch query -> Squeal IR
```

- [ ] **REST API**: ElasticSearch REST endpoints
- [ ] **Query DSL**: Convert ES queries to Squeal IR
- [ ] **Aggregations**: ES-style aggregations

## MySQL Protocol v2 (v0.8.x) - 🏗 IN PROGRESS

### High Priority

- [ ] **Real password authentication**: Implement `mysql_native_password` instead of accepting any credentials
- [ ] **Fix SQL injection**: `COM_FIELD_LIST` directly interpolates table name - use parameterized queries
- [ ] **Fix COM_INIT_DB**: Session should use selected database context

### Medium Priority

- [ ] **Proper column type mapping**: Map Squeal `Value` types to MySQL wire protocol types (INT, BIGINT, DECIMAL, etc.)
- [ ] **COM_STMT_EXECUTE**: Implement binary protocol parameter binding for prepared statements
- [ ] **Missing commands**: `USE db`, `SHOW TABLES`, `SHOW DATABASES`, `COM_CREATE_DB`, `COM_DROP_DB`
- [ ] **SSL/TLS**: Implement actual TLS handshake (capability flags advertise it but handshake missing)
- [ ] **Real metrics**: `SHOW STATUS` with actual `Uptime`, `Threads_connected`, `Questions` values

### Low Priority

- [ ] **Character set handling**: Proper UTF-8 encoding/decoding based on connection charset
- [ ] **Server version consistency**: Match `@@version` with server greeting string

## Phase 9: Production & Distributed (v1.0) - 🏗 IN PROGRESS

- [ ] **Distributed Mode**: Multi-node replication via Raft consensus
- [ ] **Telemetry**: Prometheus metrics and OpenTelemetry tracing
- [ ] **Encryption**: TLS support for HTTP and MySQL TCP protocols
- [ ] **Query Optimizer Phase 2**: Cost-based Join ordering
- [ ] JavaScript Query Interface (QuickJS)

## High-Impact Refactorings - ✅ COMPLETE

- [x] **Squeal IR**: Introduce an internal query representation layer to decouple parser from executor.
- [x] **Command Pattern Dispatcher**: Split `exec_stmt` into specialized `StatementExecutor` structs.
- [x] **Session Management**: Introduce a `Session` struct to encapsulate user, transaction, and settings state.
- [x] **Evaluator Decomposition**: Split monolithic evaluators into specialized, chainable components.
- [x] **Storage Decoupling**: Separate `Table` into `TableSchema`, `TableData`, and `TableIndexes`.
- [x] **Error Handling Unification**: Streamline `SqlError` and `StorageError` hierarchy.
