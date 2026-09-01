#!/usr/bin/env python3
"""Validate and compare Paro and DuckDB on one TPC-DS query at a time."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable

import duckdb
import _duckdb
import psycopg
from psycopg import sql

from tpcds import content_digest, corpus_digest, normalize_rows, preview, repository_revision


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="host=127.0.0.1 port=6432 dbname=postgres user=paro")
    parser.add_argument("--duckdb-database", type=Path, required=True)
    parser.add_argument("--query-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=99)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--server-binary", type=Path)
    return parser.parse_args()


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def extension_digest(module: Any) -> dict[str, str | None]:
    module_path = Path(module.__file__).resolve()
    return {
        "path": str(module_path),
        "sha256": content_digest(module_path) if module_path.is_file() else None,
    }


def timed_fetch(execute: Callable[[], list[tuple[Any, ...]]]) -> tuple[list[tuple[Any, ...]], float]:
    started = time.perf_counter_ns()
    rows = execute()
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return rows, elapsed_ms


def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    rank = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * fraction)))
    return ordered[rank]


def timing_summary(samples: list[float]) -> dict[str, Any]:
    return {
        "samples_ms": [round(sample, 6) for sample in samples],
        "minimum_ms": round(min(samples), 6),
        "median_ms": round(statistics.median(samples), 6),
        "p95_ms": round(percentile(samples, 0.95), 6),
    }


def duckdb_rows_as_expected(rows: list[tuple[Any, ...]]) -> list[list[str]]:
    return [["NULL" if value is None else str(value) for value in row] for row in rows]


def result_digest(rows: Counter[tuple[Any, ...]]) -> str:
    digest = hashlib.sha256()
    for row, count in sorted(rows.items(), key=lambda item: repr(item[0])):
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(count).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def main() -> int:
    args = parse_args()
    if not 1 <= args.start <= args.end <= 99:
        raise SystemExit("query range must satisfy 1 <= start <= end <= 99")
    if args.warmups < 0 or args.iterations < 1:
        raise SystemExit("warmups must be non-negative and iterations must be positive")

    report: dict[str, Any] = {
        "schema_version": 1,
        "corpus": "TPC-DS",
        "scale_factor": 1,
        "query_range": [args.start, args.end],
        "source": repository_revision(),
        "query_corpus_sha256": corpus_digest(args.query_dir, ".sql"),
        "duckdb_database": {
            "path": str(args.duckdb_database.resolve()),
            "sha256": content_digest(args.duckdb_database),
        },
        "configuration": {
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "statement_timeout_seconds": args.statement_timeout_seconds,
            "warmups": args.warmups,
            "iterations": args.iterations,
            "optimizer_verify": True,
            "timing_scope": "execute_and_fetch_all_rows",
            "measurement_order": "alternating_by_query_and_iteration",
        },
        "duckdb": {
            "version": duckdb.__version__,
            "python_extension": extension_digest(_duckdb),
        },
        "queries": [],
    }
    if args.server_binary is not None:
        report["server_binary"] = {
            "path": str(args.server_binary.resolve()),
            "sha256": content_digest(args.server_binary),
        }

    failures = 0
    paro = psycopg.connect(args.dsn, autocommit=True)
    duck = duckdb.connect(str(args.duckdb_database), read_only=True)
    try:
        with paro.cursor() as cursor:
            cursor.execute("SET optimizer_verify = true")
            cursor.execute(sql.SQL("SET threads = {}").format(sql.Literal(args.threads)))
            cursor.execute(sql.SQL("SET memory_limit = {}").format(sql.Literal(args.memory_limit)))
            cursor.execute(
                sql.SQL("SET statement_timeout = {}").format(
                    sql.Literal(args.statement_timeout_seconds * 1000)
                )
            )
        duck.execute(f"SET threads={args.threads}")
        duck.execute("SET memory_limit=?", [args.memory_limit])

        def run_paro(query: str) -> list[tuple[Any, ...]]:
            with paro.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

        def run_duckdb(query: str) -> list[tuple[Any, ...]]:
            return duck.execute(query).fetchall()

        for query_number in range(args.start, args.end + 1):
            query_id = f"{query_number:02d}"
            query = (args.query_dir / f"{query_id}.sql").read_text(encoding="utf-8")
            result: dict[str, Any] = {"query": query_id}
            try:
                paro_rows = run_paro(query)
                duckdb_rows = run_duckdb(query)
                if any(len(row) != len(paro_rows[0]) for row in paro_rows[1:]):
                    raise AssertionError("Paro returned inconsistent row widths")
                duckdb_width = len(duckdb_rows[0]) if duckdb_rows else 0
                paro_width = len(paro_rows[0]) if paro_rows else duckdb_width
                if duckdb_rows and any(len(row) != duckdb_width for row in duckdb_rows):
                    raise AssertionError("DuckDB returned inconsistent row widths")
                if paro_width != duckdb_width and paro_rows and duckdb_rows:
                    raise AssertionError(
                        f"schema width mismatch: Paro={paro_width}, DuckDB={duckdb_width}"
                    )
                actual, expected = normalize_rows(
                    paro_rows, duckdb_rows_as_expected(duckdb_rows), paro_width
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

                for _ in range(args.warmups):
                    run_paro(query)
                    run_duckdb(query)

                samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
                runners = {"paro": run_paro, "duckdb": run_duckdb}
                for iteration in range(args.iterations):
                    order = ["paro", "duckdb"]
                    if (query_number + iteration) % 2:
                        order.reverse()
                    for engine in order:
                        measured_rows, elapsed_ms = timed_fetch(
                            lambda engine=engine: runners[engine](query)
                        )
                        if len(measured_rows) != len(paro_rows):
                            raise AssertionError(
                                f"{engine} row count changed during measurement"
                            )
                        samples[engine].append(elapsed_ms)

                paro_timing = timing_summary(samples["paro"])
                duckdb_timing = timing_summary(samples["duckdb"])
                ratio = paro_timing["median_ms"] / duckdb_timing["median_ms"]
                result.update(
                    status="passed",
                    rows=len(paro_rows),
                    result_sha256=result_digest(actual),
                    paro=paro_timing,
                    duckdb=duckdb_timing,
                    paro_over_duckdb=round(ratio, 6),
                    faster_than_duckdb=ratio < 1,
                )
            except Exception as error:
                failures += 1
                result.update(status="failed", error=f"{type(error).__name__}: {error}")
            report["queries"].append(result)
            report["passed"] = len(report["queries"]) - failures
            report["failed"] = failures
            report["faster_than_duckdb"] = sum(
                query.get("faster_than_duckdb", False) for query in report["queries"]
            )
            write_report(args.report, report)
            if result["status"] == "passed":
                print(
                    f"TPC-DS {query_id}: Paro {result['paro']['median_ms']:.3f} ms, "
                    f"DuckDB {result['duckdb']['median_ms']:.3f} ms, "
                    f"ratio {result['paro_over_duckdb']:.3f}",
                    flush=True,
                )
            else:
                print(f"TPC-DS {query_id}: failed: {result['error']}", flush=True)
    finally:
        duck.close()
        paro.close()

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
