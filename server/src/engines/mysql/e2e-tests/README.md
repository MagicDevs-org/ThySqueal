# E2E Test Results

## Test Summary

### ThySqueal (HTTP port 8888)

| Test | Status | MySQL Feature |
|------|--------|--------------|
| basic_select | ✅ PASS | CREATE, INSERT, SELECT, ORDER BY |
| test_insert_select | ❌ FAIL | Multiple INSERT |
| test_update | ❌ FAIL | UPDATE |
| test_delete | ❌ FAIL | DELETE |
| test_datetime | ❌ FAIL | NOW(), DATETIME |
| test_aggregations | ⚠️ SKIP | AVG, SUM, COUNT (no expected) |
| test_join | ❌ FAIL | JOIN (INNER) |
| test_order_by | ❌ FAIL | ORDER BY DESC |
| test_window | ❌ FAIL | ROW_NUMBER() |

### Real MySQL (port 3306)

| Test | Status | MySQL Feature |
|------|--------|--------------|
| basic_select | ✅ PASS | CREATE, INSERT, SELECT, ORDER BY |
| test_insert_select | ✅ PASS | Multiple INSERT |
| test_update | ✅ PASS | UPDATE |
| test_delete | ✅ PASS | DELETE |
| test_datetime | ✅ PASS | NOW(), DATETIME |
| test_aggregations | ✅ PASS | AVG, SUM, COUNT |
| test_join | ✅ PASS | JOIN (INNER) |
| test_order_by | ✅ PASS | ORDER BY DESC |
| test_window | ✅ PASS | ROW_NUMBER() |

## Test Details

### basic_select
Basic table creation, insert, and select.
- **Features**: CREATE TABLE, INSERT, SELECT, ORDER BY
- **ThySqueal**: ✅ PASS

### test_insert_select
Multiple rows insert.
- **Features**: Multiple INSERT statements
- **ThySqueal**: ❌ FAIL - "key must be a string" (PRIMARY KEY required)
- **Real MySQL**: ✅ PASS

### test_update
Update existing rows.
- **Features**: UPDATE ... WHERE
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

### test_delete
Delete rows.
- **Features**: DELETE ... WHERE
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

### test_datetime
Datetime functions.
- **Features**: NOW(), DATETIME type
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

### test_aggregations
Aggregate functions.
- **Features**: AVG(), SUM(), COUNT()
- **ThySqueal**: ⚠️ SKIP - no expected file
- **Real MySQL**: ✅ PASS

### test_join
INNER JOIN between tables.
- **Features**: JOIN ... ON
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

### test_order_by
Ordering results.
- **Features**: ORDER BY DESC
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

### test_window
Window functions.
- **Features**: ROW_NUMBER() OVER()
- **ThySqueal**: ❌ FAIL
- **Real MySQL**: ✅ PASS

## Known Issues (ThySqueal)

1. **PRIMARY KEY required** - Tables without PRIMARY KEY fail with "key must be a string" error on INSERT
2. **DROP TABLE IF EXISTS** not supported - parser error on "IF"
3. **Data persists between runs** - Need to drop tables before each test

## Running Tests

### ThySqueal (HTTP on port 8888)
```bash
# Clean start
rm -rf server/data
cargo run -p thysqueal-server -- -c server/src/engines/mysql/e2e-tests/test-config.yaml &

# Run tests
python3 run_tests_http.py
```

### Real MySQL (port 3306)
```bash
# Create test database first
mysql -h localhost -P 3306 -u root -pmysql123 -e "CREATE DATABASE IF NOT EXISTS test"

# Run tests
REAL_MYSQL=true MYSQL_PWD=mysql123 python3 run_tests.py
```

## Adding New Tests

1. Create SQL file in `sql_files/`:
   ```
   sql_files/test_new_feature.sql
   ```

2. Create expected output in `expected/`:
   ```
   expected/test_new_feature.txt  # tab-separated
   # or
   expected/test_new_feature.csv  # comma-separated
   ```

3. For expected errors:
   ```
   expected/test_new_feature.err
   ```

4. Run tests to verify:
   ```bash
   # ThySqueal
   python3 run_tests_http.py

   # Real MySQL
   REAL_MYSQL=true MYSQL_PWD=mysql123 python3 run_tests.py
   ```
