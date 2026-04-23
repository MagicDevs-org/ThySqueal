#!/usr/bin/env python3
"""
End-to-end tests for ThySqueal via HTTP API.
Uses urllib (built-in) instead of requests.
"""
import os
import sys
import csv
import json
from pathlib import Path
import urllib.request
import urllib.error

HOST = os.environ.get("MYSQL_HOST", "localhost")
HTTP_PORT = os.environ.get("HTTP_PORT", "8888")

E2E_DIR = Path(__file__).parent
TESTS_DIR = E2E_DIR / "sql_files"
OUTPUT_DIR = E2E_DIR / "expected"


def execute_sql(sql: str) -> dict:
    """Execute SQL via HTTP and return JSON response."""
    data = json.dumps({"sql": sql}).encode("utf-8")
    req = urllib.request.Request(
        f"http://{HOST}:{HTTP_PORT}/_query",
        data=data,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"success": False, "error": f"HTTP {e.code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_sql_file(sql_file: Path) -> tuple[dict, list]:
    """Execute SQL file (multiple statements)."""
    with open(sql_file, "r") as f:
        sql_content = f.read()

    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    results = []

    for stmt in statements:
        if not stmt:
            continue
        results.append(execute_sql(stmt))

    final_result = results[-1] if results else {"success": False, "error": "No statements"}
    return final_result, results


def run_setup(setup_sql: str) -> None:
    """Run setup SQL (ignores errors)."""
    for stmt in [s.strip() for s in setup_sql.split(";") if s.strip()]:
        if stmt:
            execute_sql(stmt)


def read_expected(file_path: Path) -> list[list]:
    with open(file_path, "r") as f:
        if file_path.suffix == ".csv":
            return list(csv.reader(f))
        else:
            return [line.strip().split("\t") for line in f.readlines() if line.strip()]


def compare_results(actual: list[list], expected: list[list]) -> bool:
    if len(actual) != len(expected):
        return False
    for a, e in zip(actual, expected):
        if len(a) != len(e):
            return False
        for av, ev in zip(a, e):
            if str(av) != str(ev):
                return False
    return True


def run_test(sql_file: Path) -> bool:
    test_name = sql_file.stem

    # Try to drop common tables before test (ignore errors)
    for table in ['users', 'orders', 'products', 'items']:
        try:
            execute_sql(f"DROP TABLE {table}")
        except:
            pass

    expected_file_txt = OUTPUT_DIR / f"{test_name}.txt"
    expected_file_csv = OUTPUT_DIR / f"{test_name}.csv"
    expected_err = OUTPUT_DIR / f"{test_name}.err"

    if not expected_file_txt.exists() and not expected_file_csv.exists() and not expected_err.exists():
        print(f"  SKIP: no expected file for {test_name}")
        return True

    try:
        json_response, all_results = run_sql_file(sql_file)

        if expected_err.exists():
            expected_error = expected_err.read_text().strip()
            return json_response.get("success") == False and expected_error in json_response.get("error", "")

        if json_response.get("success") != True:
            print(f"  ERROR: {json_response.get('error')}")
            return False

        actual_data = json_response.get("data", [])
        actual_flat = []
        for row in actual_data:
            if isinstance(row, list):
                actual_flat.append([str(v) for v in row])
            else:
                actual_flat.append([str(row)])

        if expected_file_txt.exists():
            expected_data = read_expected(expected_file_txt)
            return compare_results(actual_flat, expected_data)
        elif expected_file_csv.exists():
            expected_data = read_expected(expected_file_csv)
            return compare_results(actual_flat, expected_data)
        else:
            return True

    except Exception as e:
        if expected_err.exists():
            expected_error = expected_err.read_text().strip()
            return expected_error in str(e)
        print(f"  ERROR: {e}")
        return False


def main():
    print(f"E2E Tests (ThySqueal HTTP, port {HTTP_PORT})")
    print("-" * 50)

    if not TESTS_DIR.exists():
        print(f"ERROR: {TESTS_DIR} not found")
        sys.exit(1)

    sql_files = sorted(TESTS_DIR.glob("*.sql"))
    if not sql_files:
        print("ERROR: no .sql files found")
        sys.exit(1)

    passed = 0
    failed = 0

    for sql_file in sql_files:
        print(f"Running {sql_file.name}...")
        if run_test(sql_file):
            print(f"  PASS")
            passed += 1
        else:
            print(f"  FAIL")
            failed += 1

    print("-" * 50)
    print(f"Results: {passed} passed, {failed} failed")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
