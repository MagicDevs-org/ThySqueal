# Comparison with Other Database Engines

This document provides a detailed technical comparison between **thy-squeal** and other database engines, with a specific focus on its alignment and differences with **MySQL 8.x**.

## Overview

thy-squeal is a **hybrid in-memory database** designed for high-performance, developer-centric workflows. It combines relational SQL, native full-text search, and Redis-compatible key-value operations into a single, memory-safe Rust binary.

| Feature | thy-squeal | SQLite | Redis | MySQL | PostgreSQL | Elasticsearch |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Model** | Relational + KV | Relational | Key-Value | Relational | Relational | Document (Search) |
| **Storage** | In-Mem (+ Sled) | Disk (B-Tree) | In-Mem (+ RDB) | Disk (B+Tree) | Disk (B-Tree) | Disk (Lucene) |
| **Language** | Rust | C | C | C++ | C | Java |
| **Full-Text Search** | Native (Tantivy) | FTS extension | RediSearch | Basic (MyISAM/Inno) | Advanced (GIN/GiST) | Native / Core |
| **Protocols** | HTTP + MySQL + RESP| C API | RESP | MySQL Binary | Postgres Binary | HTTP JSON |
| **Joins** | Inner/Left | Full Support | No | Full Support | Full Support | Limited |
| **ACID** | Supported (WAL) | Full Support | Limited | Full Support | Full Support | No |

---

## SQL Feature Comparison (thy-squeal vs. MySQL 8.x)

While thy-squeal implements a significant subset of the MySQL dialect, it is optimized for modern application patterns rather than 100% legacy compatibility.

### Core SQL Support

| Feature | thy-squeal (v0.6+) | MySQL (8.x) | Notes |
| :--- | :--- | :--- | :--- |
| **SELECT / INSERT / UPDATE / DELETE** | ✅ Full Support | ✅ Full Support | Standard DML is fully compatible. |
| **JOINs (INNER / LEFT)** | ✅ Supported | ✅ Full Support | `RIGHT` and `FULL` joins are currently not supported in thy-squeal. |
| **Subqueries** | ✅ Supported | ✅ Full Support | Supports `IN (...)` and scalar subqueries in `SELECT` and `WHERE`. |
| **CTEs (WITH clause)** | ✅ Supported | ✅ Supported | Non-recursive CTEs are fully supported. |
| **Recursive CTEs** | ❌ No | ✅ Supported | thy-squeal does not yet support `WITH RECURSIVE`. |
| **Window Functions** | ❌ No | ✅ Supported | Functions like `RANK()`, `ROW_NUMBER()`, `OVER()` are not implemented. |
| **Aggregations** | ✅ Supported | ✅ Full Support | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` and `HAVING`. |
| **Set Operations** | ❌ No | ✅ Supported | `UNION`, `INTERSECT`, `EXCEPT` are currently missing. |

### Schema & Constraints

| Feature | thy-squeal (v0.6+) | MySQL (8.x) | Notes |
| :--- | :--- | :--- | :--- |
| **Data Types** | ✅ Modern Set | ✅ Exhaustive | thy-squeal supports standard types + `JSON` and `SERIAL`. |
| **AUTO_INCREMENT** | ✅ Supported | ✅ Supported | Identical behavior for automated ID generation. |
| **Primary / Foreign Keys** | ✅ Supported | ✅ Supported | Full referential integrity checks on mutations. |
| **Unique Constraints** | ✅ Supported | ✅ Supported | Enforced via unique indexes. |
| **ALTER TABLE** | ✅ Robust | ✅ Full Support | Supports `ADD`, `DROP`, `RENAME`, `MODIFY` columns and table renaming. |
| **Materialized Views** | ✅ Supported | ❌ No (Workarounds) | thy-squeal has first-class `CREATE MATERIALIZED VIEW` support. |
| **Information Schema** | ✅ Supported | ✅ Supported | Metadata accessible via `information_schema` tables. |

### Indexing & Performance

| Feature | thy-squeal (v0.6+) | MySQL (8.x) | Notes |
| :--- | :--- | :--- | :--- |
| **B-Tree Indexes** | ✅ Supported | ✅ Default | Standard range and equality indexing. |
| **Hash Indexes** | ✅ Supported | ⚠️ Memory Only | thy-squeal supports persistent Hash indexes for O(1) lookups. |
| **Composite Indexes** | ✅ Supported | ✅ Supported | Multi-column indexing supported. |
| **Functional Indexes** | ✅ Supported | ✅ Supported | Indexing the result of an expression. |
| **Partial Indexes** | ✅ Supported | ❌ No | Indexing a subset of rows using a `WHERE` clause. |
| **JSON Path Indexes** | ✅ Supported | ⚠️ Virtual Col | Native indexing of nested JSON fields. |

### Advanced Capabilities

| Feature | thy-squeal (v0.6+) | MySQL (8.x) | Notes |
| :--- | :--- | :--- | :--- |
| **Full-Text Search** | ✅ Native (SEARCH) | ⚠️ Basic (MATCH) | thy-squeal uses Tantivy for high-performance TF-IDF search. |
| **Transactions (ACID)** | ✅ Snapshot | ✅ Multi-level | thy-squeal uses WAL and snapshotting for ACID compliance. |
| **Prepared Statements** | ✅ Supported | ✅ Supported | `PREPARE`, `EXECUTE`, `DEALLOCATE` commands. |
| **User & RBAC** | ✅ Supported | ✅ Full Support | `GRANT`, `REVOKE`, and privilege-based access control. |
| **Key-Value API** | ✅ RESP (Redis) | ❌ No | thy-squeal exposes a Redis-compatible port (6379). |
| **JSON Query (JSqueal)**| ✅ Native | ❌ No | Programmatic JSON-to-IR interface bypassing the SQL parser. |
| **Stored Procs / Triggers**| ❌ No | ✅ Supported | Logic is preferred in the application layer. |

---

## Detailed Comparison: thy-squeal vs. MySQL

### 1. Storage Architecture
- **MySQL**: Primarily uses InnoDB, which is a disk-oriented B+Tree engine. It relies heavily on a buffer pool to cache pages in memory.
- **thy-squeal**: An in-memory first engine. Data resides in optimized Rust structures for maximum speed, with durability provided by a Write-Ahead Log (WAL) and periodic snapshots via Sled.

### 2. Full-Text Search
- **MySQL**: Full-text search is available via `MATCH() AGAINST()` on InnoDB/MyISAM tables. It is functional but often lacks the performance and features of dedicated search engines.
- **thy-squeal**: Integrates **Tantivy** (a Lucene-inspired Rust library) directly into the core. The `SEARCH` command provides professional-grade search capabilities (stemming, ranking, tokenization) alongside relational joins.

### 3. Protocol & Integration
- **MySQL**: Uses the MySQL binary protocol. Requires specific drivers for every language.
- **thy-squeal**: Triple-threat connectivity:
    - **MySQL Protocol**: Use any existing MySQL client/ORM.
    - **HTTP JSON API**: Perfect for serverless, web, and quick integrations without persistent connections.
    - **RESP (Redis)**: High-speed key-value operations using standard Redis clients.

### 4. Schema Evolution
- **MySQL**: `ALTER TABLE` operations in MySQL can be complex (Online DDL vs. Copy).
- **thy-squeal**: `ALTER TABLE` is designed to be lightweight. Since data is in-memory, structural changes like adding or dropping columns are extremely fast.

### 5. Programming Interface
- **MySQL**: SQL is the only primary interface.
- **thy-squeal**: Introduces **JSqueal**, a JSON representation of the query IR. This allows developers to build complex queries programmatically without string concatenation or SQL injection risks, while still having the option of standard SQL.

---

## Summary: When to Use What?

| Use Case | Recommended Engine | Why? |
| :--- | :--- | :--- |
| **High-Scale Web Apps** | **MySQL** | Battle-tested replication, massive ecosystem, and specialized hosting. |
| **Search-Heavy Apps** | **thy-squeal** | Native Tantivy integration saves you from running Elasticsearch alongside MySQL. |
| **Microservices / Edge** | **thy-squeal** | Lightweight binary, HTTP API, and combined SQL/KV capabilities. |
| **Complex Reporting** | **PostgreSQL** | Best-in-class support for Window Functions and recursive queries. |
| **Simple KV Caching** | **Redis / thy-squeal** | Low latency; thy-squeal is better if you eventually need to query that data with SQL. |
