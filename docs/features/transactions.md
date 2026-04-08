# Transactions

## Overview

Support for ACID transactions in ThySqueal, allowing multiple SQL statements to be executed as a single atomic unit.

**Status**: ✅ Implemented (Snapshot Isolation via CoW)

## Implementation Details

### Copy-on-Write (CoW) State

When a transaction is started with `BEGIN`, ThySqueal creates a lightweight clone of the entire `DatabaseState`. This state is stored in a session-specific map indexed by a `transaction_id`.

- **Isolation**: Each transaction operates on its own private snapshot. Changes are not visible to other transactions or the global state until committed.
- **Atomicity**: Either all changes in the transaction are applied (via `COMMIT`) or none are (via `ROLLBACK`).
- **Consistency**: Unique constraints and data types are validated against the private state before being merged back to the global state.

### Multi-Request HTTP Sessions

Since HTTP is stateless, ThySqueal uses a `transaction_id` to link multiple requests into a single transaction.

1. **BEGIN**: Returns a unique `transaction_id`.
2. **Operations**: All subsequent queries must include this `transaction_id`.
3. **COMMIT/ROLLBACK**: Finalizes the transaction and cleans up the session state.

---

## HTTP API Usage

### 1. Start Transaction

```bash
POST /_query
{ "sql": "BEGIN" }
```

**Response**:

```json
{
  "success": true,
  "transaction_id": "tx_abc_123",
  "data": []
}
```

### 2. Execute Operations

```bash
POST /_query
{
  "sql": "INSERT INTO users VALUES (1, 'Alice')",
  "transaction_id": "tx_abc_123"
}
```

### 3. Commit

```bash
POST /_query
{
  "sql": "COMMIT",
  "transaction_id": "tx_abc_123"
}
```

### 4. Rollback (Alternative)

```bash
POST /_query
{
  "sql": "ROLLBACK",
  "transaction_id": "tx_abc_123"
}
```
