# MySQL E2E Test Suite

Comprehensive MySQL feature tests for ThySqueal compatibility.

## Test Files

### Data Types
| Test File | Feature |
|----------|--------|
| basic_select | CREATE, INSERT, SELECT, ORDER BY |
| test_data_types | VARCHAR, TEXT, DATETIME, DATE, TIME |
| test_nulls | NULL handling, IFNULL, COALESCE, IF |

### DDL
| Test File | Feature |
|----------|--------|
| test_ddl | ALTER TABLE (ADD, MODIFY, CHANGE) |

### DML
| Test File | Feature |
|----------|--------|
| test_insert_select | Multiple INSERT |
| test_update | UPDATE ... WHERE |
| test_delete | DELETE ... WHERE |

### Queries
| Test File | Feature |
|----------|--------|
| test_where | WHERE, AND, OR, IN, BETWEEN |
| test_subquery | Subqueries |
| test_group_by | GROUP BY, HAVING |
| test_order_by | ORDER BY DESC |
| test_join | INNER JOIN |
| test_join_all | LEFT/RIGHT JOIN |

### Functions
| Test File | Feature |
|----------|--------|
| test_string_funcs | CONCAT, LENGTH, UPPER, LOWER, SUBSTRING, REPLACE |
| test_math_funcs | ABS, ROUND, POW, SQRT, MOD |
| test_date_funcs | NOW, DATE_FORMAT, DATEDIFF, DATE_ADD |
| test_aggregations | AVG, SUM, COUNT |
| test_window | ROW_NUMBER, RANK, LEAD/LAG |

### Advanced
| Test File | Feature |
|----------|--------|
| test_union | UNION, UNION ALL |
| test_view | CREATE/DROP VIEW |
| test_index | CREATE/DROP INDEX |
| test_transaction | BEGIN, COMMIT, ROLLBACK |

## Running Tests

### ThySqueal (HTTP on port 8888)
```bash
# Start server
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

## Test Summary (Real MySQL)

| Feature | Status |
|---------|--------|
| CREATE/INSERT/SELECT | ✅ |
| Data Types | ✅ |
| NULL handling | ✅ |
| ALTER TABLE | ✅ |
| UPDATE | ✅ |
| DELETE | ✅ |
| WHERE clauses | ✅ |
| Subqueries | ✅ |
| GROUP BY | ✅ |
| ORDER BY | ✅ |
| JOIN (INNER/LEFT/RIGHT) | ✅ |
| String functions | ✅ |
| Math functions | ✅ |
| Date functions | ✅ |
| Aggregations | ✅ |
| Window functions | ✅ |
| UNION | ✅ |
| Views | ✅ |
| Indexes | ✅ |
| Transactions | ✅ |
