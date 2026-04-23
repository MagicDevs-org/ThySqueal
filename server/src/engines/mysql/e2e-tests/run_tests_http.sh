#!/bin/bash
#
# End-to-end tests for ThySqueal via HTTP API
#
set -e

HOST="${MYSQL_HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-8888}"
E2E_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTS_DIR="$E2E_DIR/sql_files"
EXPECTED_DIR="$E2E_DIR/expected"

echo "E2E Tests (ThySqueal HTTP, port $HTTP_PORT)"
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

    # Execute each statement in the file
    last_response=""
    while IFS=';' read -r stmt; do
        stmt=$(echo "$stmt" | xargs)
        [ -z "$stmt" ] && continue

        stmt_json=$(echo "$stmt" | jq -Rs .)
        last_response=$(curl -s -X POST "http://$HOST:$HTTP_PORT/_query" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": $stmt_json}")
    done < "$sql_file"

    # Parse final response
    success=$(echo "$last_response" | jq -r '.success // "false"')

    # Check for error file
    if [ -f "$expected_err" ]; then
        if [ "$success" = "false" ]; then
            error_msg=$(echo "$last_response" | jq -r '.error // ""')
            expected_error=$(cat "$expected_err")
            if echo "$error_msg" | grep -q "$expected_error"; then
                echo "PASS"
                ((passed++))
            else
                echo "FAIL (got: $error_msg)"
                ((failed++))
            fi
        else
            echo "FAIL (expected error)"
            ((failed++))
        fi
        continue
    fi

    if [ "$success" != "true" ]; then
        echo "FAIL (error: $(echo "$last_response" | jq -r '.error'))"
        ((failed++))
        continue
    fi

    # Compare data
    if [ -f "$expected_txt" ]; then
        actual=$(echo "$last_response" | jq -r '.data[][] | tostring' | sort | tr '\n' '\t')
        expected=$(cat "$expected_txt" | sort | tr '\n' '\t')

        if [ "$actual" = "$expected" ]; then
            echo "PASS"
            ((passed++))
        else
            echo "FAIL"
            ((failed++))
        fi
    elif [ -f "$expected_csv" ]; then
        actual=$(echo "$last_response" | jq -r '.data[] | @csv' | sort)
        expected=$(cat "$expected_csv" | sort)

        if [ "$actual" = "$expected" ]; then
            echo "PASS"
            ((passed++))
        else
            echo "FAIL"
            ((failed++))
        fi
    else
        echo "PASS"
        ((passed++))
    fi
done

echo "--------------------------------------------"
echo "Results: $passed passed, $failed failed"

if [ "$failed" -gt 0 ]; then
    exit 1
fi
exit 0
