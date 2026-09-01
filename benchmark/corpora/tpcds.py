#!/usr/bin/env python3
"""Execute the DuckDB TPC-DS corpus against Paro and verify every row multiset."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import time
from collections import Counter
from datetime import date, datetime, time as datetime_time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

import psycopg
from psycopg import sql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="host=127.0.0.1 port=6432 dbname=postgres user=paro")
    parser.add_argument("--query-dir", type=Path, required=True)
    parser.add_argument("--answer-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=99)
    parser.add_argument("--statement-timeout-seconds", type=int, default=180)
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument(
        "--server-binary",
        type=Path,
        help="local parod binary used for this run; records its content digest",
    )
    return parser.parse_args()


def content_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def corpus_digest(path: Path, suffix: str) -> str:
    digest = hashlib.sha256()
    for item in sorted(path.glob(f"*{suffix}")):
        digest.update(item.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(content_digest(item)))
    return digest.hexdigest()


def repository_revision() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]

    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", *arguments], cwd=root, text=True, stderr=subprocess.DEVNULL
        ).strip()

    try:
        return {
            "commit": git("rev-parse", "HEAD"),
            "dirty": bool(git("status", "--porcelain")),
        }
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "dirty": None}


def actual_converter(values: list[Any]) -> Callable[[Any], Any]:
    sample = next((value for value in values if value is not None), None)
    if sample is None:
        return lambda value: None if value is None else str(value)
    if isinstance(sample, bool):
        return lambda value: None if value is None else bool(value)
    if isinstance(sample, int):
        return lambda value: None if value is None else int(value)
    if isinstance(sample, Decimal):
        return lambda value: None if value is None else Decimal(value)
    if isinstance(sample, float):
        return lambda value: None if value is None else round(float(value), 10)
    if isinstance(sample, (date, datetime, datetime_time)):
        return lambda value: None if value is None else value.isoformat()
    if isinstance(sample, bytes):
        return lambda value: None if value is None else bytes(value).hex()
    return lambda value: None if value is None else str(value)


def expected_converter(actual: Callable[[Any], Any], sample: Any) -> Callable[[str], Any]:
    if sample is None:
        return lambda value: None if value == "NULL" else value
    if isinstance(sample, bool):
        return lambda value: None if value == "NULL" else value.lower() in {"true", "t", "1"}
    if isinstance(sample, int):
        return lambda value: None if value == "NULL" else int(value)
    if isinstance(sample, Decimal):
        return lambda value: None if value == "NULL" else Decimal(value)
    if isinstance(sample, float):
        return lambda value: None if value == "NULL" else round(float(value), 10)
    return lambda value: None if value == "NULL" else value


def normalize_rows(
    actual_rows: list[tuple[Any, ...]], expected_rows: list[list[str]], column_count: int
) -> tuple[Counter[tuple[Any, ...]], Counter[tuple[Any, ...]]]:
    actual_columns = [
        [row[index] for row in actual_rows] for index in range(column_count)
    ]
    actual_converters = [actual_converter(values) for values in actual_columns]
    samples = [next((value for value in values if value is not None), None) for values in actual_columns]
    expected_converters = [
        expected_converter(actual_converters[index], samples[index])
        for index in range(column_count)
    ]
    actual = Counter(
        tuple(actual_converters[index](value) for index, value in enumerate(row))
        for row in actual_rows
    )
    expected = Counter(
        tuple(expected_converters[index](value) for index, value in enumerate(row))
        for row in expected_rows
    )
    return actual, expected


def read_answer(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle, delimiter="|", quotechar='"'))
    if not rows:
        raise ValueError(f"answer file is empty: {path}")
    width = len(rows[0])
    for index, row in enumerate(rows[1:], start=2):
        if len(row) != width:
            raise ValueError(f"answer row width mismatch: {path}:{index}")
    return rows[0], rows[1:]


def preview(counter: Counter[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {"row": [str(value) if value is not None else None for value in row], "count": count}
        for row, count in counter.most_common(5)
    ]


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    args = parse_args()
    if not 1 <= args.start <= args.end <= 99:
        raise SystemExit("query range must satisfy 1 <= start <= end <= 99")

    report: dict[str, Any] = {
        "corpus": "TPC-DS",
        "scale_factor": 0.01,
        "query_range": [args.start, args.end],
        "source": repository_revision(),
        "query_corpus_sha256": corpus_digest(args.query_dir, ".sql"),
        "answer_corpus_sha256": corpus_digest(args.answer_dir, ".csv"),
        "configuration": {
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "statement_timeout_seconds": args.statement_timeout_seconds,
            "optimizer_verify": True,
        },
        "validation": {
            "rows": "multiset",
            "ordering": "not_checked",
            "types": "normalized_from_actual_values",
        },
        "queries": [],
    }
    if args.server_binary is not None:
        report["server_binary"] = {
            "path": str(args.server_binary.resolve()),
            "sha256": content_digest(args.server_binary),
        }
    failures = 0
    with psycopg.connect(args.dsn, autocommit=True) as connection:
        report["server"] = {
            "host": connection.info.host,
            "port": connection.info.port,
            "database": connection.info.dbname,
            "user": connection.info.user,
        }
        with connection.cursor() as cursor:
            cursor.execute("SET optimizer_verify = true")
            cursor.execute(sql.SQL("SET threads = {}").format(sql.Literal(args.threads)))
            cursor.execute(
                sql.SQL("SET memory_limit = {}").format(sql.Literal(args.memory_limit))
            )
            cursor.execute(
                sql.SQL("SET statement_timeout = {}").format(
                    sql.Literal(args.statement_timeout_seconds * 1000)
                )
            )

        for query_number in range(args.start, args.end + 1):
            query_id = f"{query_number:02d}"
            query_path = args.query_dir / f"{query_id}.sql"
            answer_path = args.answer_dir / f"{query_id}.csv"
            started = time.monotonic()
            result: dict[str, Any] = {"query": query_id}
            try:
                expected_names, expected_rows = read_answer(answer_path)
                with connection.cursor() as cursor:
                    cursor.execute(query_path.read_text(encoding="utf-8"))
                    actual_rows = cursor.fetchall()
                    actual_names = [column.name for column in cursor.description or ()]
                # DuckDB's generated answer files carry engine-specific
                # display labels for unaliased expressions and qualified
                # references. SQL requires the result arity and values here;
                # it does not require another engine to reproduce those
                # presentation strings. Keep the names in the report for
                # diagnostics, but do not turn dialect spelling into a plan
                # correctness failure.
                if len(actual_names) != len(expected_names):
                    raise AssertionError(
                        f"schema width mismatch: expected={len(expected_names)}, "
                        f"actual={len(actual_names)}"
                    )
                if actual_names != expected_names:
                    result["expected_names"] = expected_names
                    result["actual_names"] = actual_names
                if any(len(row) != len(expected_names) for row in actual_rows):
                    raise AssertionError("Paro returned a row with the wrong width")
                actual, expected = normalize_rows(
                    actual_rows, expected_rows, len(expected_names)
                )
                missing = expected - actual
                unexpected = actual - expected
                if missing or unexpected:
                    result["missing"] = preview(missing)
                    result["unexpected"] = preview(unexpected)
                    raise AssertionError(
                        f"row multiset mismatch: missing={sum(missing.values())}, "
                        f"unexpected={sum(unexpected.values())}"
                    )
                result.update(status="passed", rows=len(actual_rows))
            except (Exception, InvalidOperation) as error:
                failures += 1
                result.update(status="failed", error=f"{type(error).__name__}: {error}")
            result["elapsed_seconds"] = round(time.monotonic() - started, 6)
            report["queries"].append(result)
            report["passed"] = len(report["queries"]) - failures
            report["failed"] = failures
            write_report(args.report, report)
            print(
                f"TPC-DS {query_id}: {result['status']} "
                f"({result['elapsed_seconds']:.3f}s)"
                + (f" rows={result.get('rows')}" if result["status"] == "passed" else f" {result['error']}"),
                flush=True,
            )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
