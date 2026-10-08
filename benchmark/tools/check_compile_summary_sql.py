#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Small real-server T1 contract probe; stdout is raw evidence, never timing data."""
import argparse
import json
from pathlib import Path

import psycopg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()
    fixtures = Path(__file__).resolve().parents[1] / "fixtures" / "compile-summary"
    expected = json.loads((fixtures / "query-summary-v3.json").read_text())
    text_expected = (fixtures / "query-summary-v3.txt").read_text().splitlines()
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        for target in ("SELECT 42", "WITH t AS (SELECT 3 AS x) SELECT x FROM t"):
            with conn.cursor() as cursor:
                cursor.execute(f"EXPLAIN (COMPILE, FORMAT JSON) {target}")
                assert [c.name for c in cursor.description] == ["QUERY PLAN"]
                rows = cursor.fetchall()
                assert len(rows) == 1
                raw = rows[0][0]
                record = json.loads(raw)
                # Golden contains only stable contract fields; raw evidence retains
                # all observed timings, identities and search outcomes unchanged.
                assert {key: record[key] for key in expected} == expected
                assert record["cache"] == "ForcedCompile"
                assert record["outcome"] == "Success"
                assert record["admission"] == record["execution"] == "NotExecuted"
                phases = ("bind_ns", "optimizer_ns", "verify_ns", "finish_ns", "compiler_other_ns")
                assert sum(record[p]["Observed"] for p in phases) == record["compiler_ns"]["Observed"]
                print(raw)
            text = conn.execute(f"EXPLAIN (COMPILE) {target}").fetchone()[0]
            assert text.startswith("EXPLAIN (COMPILE)")
            assert "execution=NotExecuted" in text
            assert [line for line in text.splitlines() if line.startswith(("EXPLAIN (COMPILE)", "admission="))] == text_expected
            print(text)
        conn.execute("CREATE TEMP TABLE compile_probe (v VARCHAR)")
        conn.execute("INSERT INTO compile_probe VALUES ('invalid-integer')")
        conn.execute("EXPLAIN (COMPILE) SELECT CAST(v AS INTEGER) FROM compile_probe")
        try:
            conn.execute("SELECT CAST(v AS INTEGER) FROM compile_probe")
        except psycopg.Error as error:
            print(json.dumps({"runtime_error_sqlstate": error.sqlstate}))
        else:
            raise AssertionError("target should fail only when executed")
        errors = []
        for prefix in ("", "EXPLAIN (COMPILE) "):
            try:
                conn.execute(prefix + "SELECT absent_compile_column")
            except psycopg.Error as error:
                errors.append((error.sqlstate, error.diag.message_primary))
        assert len(errors) == 2 and errors[0] == errors[1]
        print(json.dumps({"error_identity_preserved": errors[0], "status": "passed"}))


if __name__ == "__main__":
    main()
