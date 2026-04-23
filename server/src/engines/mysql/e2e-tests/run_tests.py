#!/usr/bin/env python3
"""
End-to-end tests for MySQL compatibility.
Uses REAL_MYSQL=true|false to choose real MySQL (port 3306) or ThySqueal (port 13306).
"""
import os
import sys
import csv
from pathlib import Path
from typing import Any

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
REAL_MYSQL = os.environ.get("REAL_MYSQL", "false").lower() == "true"
MYSQL_PWD = os.environ.get("MYSQL_PWD", "mysql123" if REAL_MYSQL else "")
MYSQL_PORT = 3306 if REAL_MYSQL else 13306

E2E_DIR = Path(__file__).parent
TESTS_DIR = E2E_DIR / "sql_files"
OUTPUT_DIR = E2E_DIR / "expected"


def get_connection() -> Any:
    """Get database connection based on REAL_MYSQL setting."""
    try:
        import mysql.connector
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user="root",
            password=MYSQL_PWD,
            database="test"
        )
    except ImportError:
        pass

    import pymysql
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user="root",
        password=MYSQL_PWD,
        database="test"
    )


def run_sql_file(sql_file: Path) -> tuple[list[str], list[list]]:
    """Execute SQL file and return results."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        with open(sql_file, "r") as f:
            sql_content = f.read()

        statements = [s.strip() for s in sql_content.split(";") if s.strip()]
        results = []

        for stmt in statements:
            if not stmt:
                continue
            cursor.execute(stmt)
            if cursor.description:
                results = cursor.fetchall()
            else:
                conn.commit()

        if cursor.description:
            cols = [desc[0] for desc in cursor.description]
            return cols, results
        return [], []

    finally:
        if conn:
            conn.close()


def read_expected(file_path: Path) -> tuple[list[str], list[list]]:
    """Read expected output from text or csv file."""
    with open(file_path, "r") as f:
        if file_path.suffix == ".csv":
            reader = csv.reader(f)
            return [], list(reader)
        else:
            lines = f.readlines()
            return [], [line.strip().split("\t") for line in lines if line.strip()]


def compare_results(actual: list[list], expected: list[list]) -> bool:
    """Compare actual vs expected results."""
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
    """Run a single test case."""
    test_name = sql_file.stem

    expected_file_txt = OUTPUT_DIR / f"{test_name}.txt"
    expected_file_csv = OUTPUT_DIR / f"{test_name}.csv"
    expected_err = OUTPUT_DIR / f"{test_name}.err"

    if not expected_file_txt.exists() and not expected_file_csv.exists() and not expected_err.exists():
        print(f"  SKIP: no expected file for {test_name}")
        return True

    try:
        actual_cols, actual_results = run_sql_file(sql_file)

        if expected_err.exists():
            expected_error = expected_err.read_text().strip()
            print(f"  (expected error: {expected_error})")
            return False

        expected_cols, expected_results = read_expected(expected_file_txt if expected_file_txt.exists() else expected_file_csv)

        if expected_results and actual_results:
            return compare_results(actual_results, expected_results)
        elif not expected_results and not actual_results:
            return True
        else:
            return False

    except Exception as e:
        if expected_err.exists():
            expected_error = expected_err.read_text().strip()
            return str(e) == expected_error
        print(f"  ERROR: {e}")
        return False


def main():
    mode = "REAL MySQL" if REAL_MYSQL else "ThySqueal"
    print(f"E2E Tests ({mode}, port {MYSQL_PORT})")
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
