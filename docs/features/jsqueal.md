# JSqueal - JSON Query Language

JSqueal is a structured JSON representation for SQL queries in **ThySqueal**. It maps directly to the **Squeal IR** (Internal Representation), bypassing the standard SQL parser (`sql.pest`).

## Architecture

1. **SQL Workflow**: `SQL String` -> `Parser (Pest)` -> `AST` -> `Squeal IR` -> `Executor`
2. **JSqueal Workflow**: `JSON` -> `Squeal IR` -> `Executor`

This decoupling allows for:
- **Programmatic Query Construction**: Easier to build complex queries in code without string manipulation.
- **Improved Security**: Reduces the risk of SQL injection as the query structure is pre-defined.
- **Protocol Flexibility**: Can be easily exposed via HTTP or other non-SQL protocols.

## HTTP API

JSqueal is accessible via the `POST /_jsqueal` endpoint.

### Request Format

```json
{
  "squeal": {
    "Select": {
      "table": "users",
      "columns": [
        { "expr": { "Column": "name" }, "alias": "user_name" },
        { "expr": { "Column": "email" } }
      ],
      "where_clause": {
        "Comparison": [
          { "Column": "id" },
          "Eq",
          { "Literal": { "Int": 1 } }
        ]
      }
    }
  },
  "transaction_id": null,
  "username": "root"
}
```

## Supported Operations

All core SQL operations are supported via JSqueal:

### 1. SELECT
```json
{
  "Select": {
    "table": "users",
    "columns": [{ "expr": "Star" }],
    "distinct": false,
    "joins": [],
    "group_by": [],
    "order_by": [{ "expr": { "Column": "id" }, "order": "Desc" }],
    "limit": { "count": 10, "offset": 0 }
  }
}
```

### 2. INSERT
```json
{
  "Insert": {
    "table": "users",
    "columns": ["id", "name"],
    "values": [
      { "Literal": { "Int": 1 } },
      { "Literal": { "Text": "Alice" } }
    ]
  }
}
```

### 3. UPDATE
```json
{
  "Update": {
    "table": "users",
    "assignments": [
      ["name", { "Literal": { "Text": "Bob" } }]
    ],
    "where_clause": {
      "Comparison": [
        { "Column": "id" },
        "Eq",
        { "Literal": { "Int": 1 } }
      ]
    }
  }
}
```

### 4. DELETE
```json
{
  "Delete": {
    "table": "users",
    "where_clause": {
      "Comparison": [
        { "Column": "id" },
        "Eq",
        { "Literal": { "Int": 1 } }
      ]
    }
  }
}
```

## Advantages

- **Strict Typing**: JSON schema validation ensures query structural integrity.
- **Performance**: Skips the expensive parsing stage.
- **Consistency**: Shares the same execution engine and optimizations as standard SQL.
