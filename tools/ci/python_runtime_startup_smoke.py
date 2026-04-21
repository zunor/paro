# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import sys

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify parod still starts and serves ordinary SQL when Python runtime is unavailable."
    )
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--password", default="")
    parser.add_argument(
        "--expected-message",
        default="Python runtime is disabled by configuration",
    )
    parser.add_argument(
        "--expected-sqlstate",
        default="39P04",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            row = cursor.fetchone()
            if row != (1,):
                raise AssertionError(f"expected SELECT 1 to return (1,), got {row!r}")

            try:
                cursor.execute(
                    """
                    CREATE FUNCTION py_disabled(a INTEGER) RETURNS INTEGER
                    LANGUAGE python
                    AS $$return a$$;
                    """
                )
            except psycopg.Error as exc:
                message = str(exc)
                if args.expected_message not in message:
                    raise AssertionError(
                        f"expected error to contain {args.expected_message!r}, got {message!r}"
                    ) from exc
                if exc.sqlstate != args.expected_sqlstate:
                    raise AssertionError(
                        f"expected SQLSTATE {args.expected_sqlstate}, got {exc.sqlstate}"
                    ) from exc
            else:
                cursor.execute("DROP FUNCTION IF EXISTS py_disabled(INTEGER);")
                raise AssertionError(
                    "CREATE FUNCTION ... LANGUAGE python unexpectedly succeeded while runtime was disabled"
                )
    finally:
        conn.close()

    print("python runtime startup smoke passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
