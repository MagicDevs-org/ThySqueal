# JavaScript REPL Client

## Overview

Interactive CLI client with embedded JavaScript runtime for scripting.

## Binary

`thysqueal-cli` - CLI tool with JS REPL

## Installation

```bash
cargo install thysqueal-cli
```

## Usage

```bash
# Interactive REPL
thysqueal-cli

# Execute SQL
thysqueal-cli -h localhost -p 3306 -e "SELECT * FROM users"

# HTTP mode
thysqueal-cli --http localhost:9200 -e "SELECT * FROM users"

# Run script
thysqueal-cli script.js

# Import/Export
thysqueal-cli --import data.json
thysqueal-cli --export data.json
```

## JavaScript API

### Connection

```javascript
const thy = require('ThySqueal');

// TCP SQL connection
const conn = thy.connect('thysqueal://localhost:3306');

// HTTP connection
const conn = thy.connect('http://localhost:9200');
```

### Query

```javascript
// Simple query
const result = conn.query('SELECT * FROM users');

// With parameters
const result = conn.query('SELECT * FROM users WHERE age > ?', [18]);

// Get rows
for (const row of result.rows) {
  console.log(row.name, row.age);
}

// Get affected count
console.log(`Deleted ${result.affected} rows`);
```

### Key-Value

```javascript
// Set value
thy.kv.set('session:123', { user: 'alice', exp: 3600 });

// Get value
const session = thy.kv.get('session:123');

// Delete
thy.kv.del('session:123');

// Increment
thy.kv.incr('counter');
```

### Full-Text Search

```javascript
const hits = conn.search('users', 'developer', {
  fields: ['name', 'bio'],
  limit: 10
});

for (const hit of hits) {
  console.log(hit.id, hit.score, hit.data);
}
```

### Transaction (Future)

```javascript
const tx = conn.begin();
try {
  tx.query('INSERT INTO orders VALUES (?, ?)', [1, 100]);
  tx.query('UPDATE stock SET qty = qty - 1 WHERE id = ?', [5]);
  tx.commit();
} catch (e) {
  tx.rollback();
}
```

## REPL Features

### Commands

```code
.help           Show help
.load script.js Load and execute JS file
.quit           Exit
.clear          Clear screen
.tables         List tables
```

### Keyboard Shortcuts

- `Ctrl+C` - Cancel current input
- `Ctrl+D` - Exit REPL
- `Up/Down` - History navigation
- `Tab` - Autocomplete

### Configuration

```yaml
# ~/.thysqueal-cli/config.yaml
connection:
  default_host: "localhost"
  default_port: 9200

repl:
  history_size: 1000
  auto_indent: true
  prompt: "thy> "
```

## Script Example

```javascript
// batch.js - Import users from JSON file
const fs = require('fs');
const data = JSON.parse(fs.readFileSync('users.json'));

for (const user of data) {
  conn.query(
    'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
    [user.name, user.email, user.age]
  );
}

console.log(`Imported ${data.length} users`);
```
