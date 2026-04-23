#!/bin/bash
#
# End-to-end tests for MySQL compatibility.
# Uses REAL_MYSQL=true|false to choose real MySQL (port 3306) or ThySqueal (port 13306).
#
set -e

MYSQL_HOST="${MYSQL_HOST:-localhost}"
REAL_MYSQL="${REAL_MYSQL:-false}"
MYSQL_PWD="${MYSQL_PWD:-}"

if [ "$REAL_MYSQL" = "true" ]; then
    MYSQL_PORT=3306
    MYSQL_PWD="${MYSQL_PWD:-mysql123}"
    CLIENT_OPTS=(--port=3306)
    MODE="REAL MySQL"
else
    MYSQL_PORT=13306
    MYSQL_PWD=""
    CLIENT_OPTS=(--port=13306)
    MODE="ThySqueal"
fi

E2E_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTS_DIR="$E2E_DIR/sql_files"
EXPECTED_DIR="$E2E_DIR/expected"
MYSQL="mysql"

echo "E2E Tests ($MODE, port $MYSQL_PORT)"
echo "--------------------------------------------"

passed=0
failed=0

for sql_file in "$TESTS_DIR"/*.sql; do
    [ -e "$sql_file" ] || continue

    test_name=$(basename "$sql_file" .sql)
    expected_txt="$EXPECTED_DIR/$test_name.txt"
    expected_csv="$EXPECTED_DIR/$test_name.csv"
    expected_err="$EXPECTED_DIR/$test_name.err"

    if [ ! -f "$expected_txt" ] && [ ! -f "$expected_csv" ] && [ ! -f "$expected_err" ]; then
        echo "SKIP: $test_name (no expected file)"
        continue
    fi

    echo -n "Running $test_name... "

    # Run SQL and capture output or error
    output=$("$MYSQL" -h "$MYSQL_HOST" "${CLIENT_OPTS[@]}" -u root -p"$MYSQL_PWD" test 2>&1 < "$sql_file" || true)

    # Check for error file
    if [ -f "$expected_err" ]; then
        if echo "$output" | grep -q "$(cat "$expected_err")"; then
            echo "PASS"
            ((passed++))
        else
            echo "FAIL"
            ((failed++))
        fi
        continue
    fi

    # Normalize output (remove empty lines, sort for comparison)
    actual=$(echo "$output" | grep -v '^$' | sort)

    if [ -f "$expected_txt" ]; then
        expected=$(cat "$expected_txt" | sort)
    elif [ -f "$expected_csv" ]; then
        expected=$(cat "$expected_csv")
        actual=$(echo "$output" | grep -v '^$')
    fi

    if [ "$actual" = "$expected" ]; then
        echo "PASS"
        ((passed++))
    else
        echo "FAIL"
        ((failed++))
    fi
done

echo "--------------------------------------------"
echo "Results: $passed passed, $failed failed"

if [ "$failed" -gt 0 ]; then
    exit 1
fi
exit 0
