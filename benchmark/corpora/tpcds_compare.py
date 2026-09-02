#!/usr/bin/env python3
"""Run one typed, process-owned Paro/DuckDB TPC-DS comparison at a time."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any, Callable

import _duckdb
import duckdb
import psycopg
from psycopg import sql

from benchmark_evidence import (
    ManagedParoServer,
    content_digest,
    paired_order_balanced_ratio,
    repository_identity,
    tree_digest,
)
from tpcds_result_contract import (
    ColumnContract,
    assert_compatible_schema,
    assert_peer_order,
    assert_same_multiset,
    canonicalize_rows,
    duckdb_schema,
    multiset_digest,
    paro_schema,
    parse_order_contract,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", type=Path, required=True)
    parser.add_argument("--server-data-dir", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:6432")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--duckdb-database", type=Path, required=True)
    parser.add_argument("--dataset-source-dir", type=Path, required=True)
    parser.add_argument("--query-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=99)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument(
        "--paro-result-format", choices=("binary", "text"), default="binary"
    )
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


def timed_fetch(
    execute: Callable[[], tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...]]]
) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...], float]:
    started = time.perf_counter_ns()
    rows, schema = execute()
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return rows, schema, elapsed_ms


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


def schema_report(schema: tuple[ColumnContract, ...]) -> list[dict[str, str]]:
    return [
        {"name": column.name, "family": column.family, "engine_type": column.engine_type}
        for column in schema
    ]


def main() -> int:
    args = parse_args()
    if not 1 <= args.start <= args.end <= 99:
        raise SystemExit("query range must satisfy 1 <= start <= end <= 99")
    if args.warmups < 0 or args.iterations < 1:
        raise SystemExit("warmups must be non-negative and iterations must be positive")

    repo_root = Path(__file__).resolve().parents[2]
    harness_files = [
        Path(__file__).resolve(),
        Path(__file__).with_name("benchmark_evidence.py").resolve(),
        Path(__file__).with_name("tpcds_result_contract.py").resolve(),
    ]
    server = ManagedParoServer(
        args.server_binary,
        args.server_data_dir,
        args.listen,
        args.report.with_suffix(".parod.log"),
    )
    report: dict[str, Any] = {
        "schema_version": 2,
        "corpus": "TPC-DS",
        "scale_factor": 1,
        "query_range": [args.start, args.end],
        "source": repository_identity(repo_root),
        "query_corpus_sha256": tree_digest(args.query_dir, (".sql",)),
        "dataset": {
            "source_path": str(args.dataset_source_dir.resolve()),
            "source_sha256": tree_digest(args.dataset_source_dir),
            "paro_data_path": str(args.server_data_dir.resolve()),
            "paro_data_sha256_before_run": tree_digest(args.server_data_dir),
            "duckdb_path": str(args.duckdb_database.resolve()),
            "duckdb_sha256": content_digest(args.duckdb_database),
        },
        "harness": {
            "files": [
                {"path": str(path), "sha256": content_digest(path)}
                for path in harness_files
            ]
        },
        "configuration": {
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "statement_timeout_seconds": args.statement_timeout_seconds,
            "warmups": args.warmups,
            "iterations": args.iterations,
            "optimizer_verify": True,
            "timing_scope": "execute_fetch_and_result_metadata",
            "validation_scope": "outside_timed_region_every_sample",
            "measurement_order": "alternating_by_query_and_iteration",
            "paro_result_format": args.paro_result_format,
        },
        "validation": {
            "rows": "typed_full_multiset_digest_every_sample",
            "ordering": "explicit_result_keys_with_peer_group_semantics",
            "schema": "metadata_name_arity_and_logical_family",
        },
        "duckdb": {
            "version": duckdb.__version__,
            "python_extension": extension_digest(_duckdb),
        },
        "queries": [],
    }

    failures = 0
    with server:
        report["paro_server"] = server.identity()
        host, port = args.listen.rsplit(":", 1)
        dsn = f"host={host} port={port} dbname={args.database} user={args.user}"
        paro = psycopg.connect(dsn, autocommit=True)
        duck = duckdb.connect(str(args.duckdb_database), read_only=True)
        try:
            with paro.cursor() as cursor:
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
            duck.execute(f"SET threads={args.threads}")
            duck.execute("SET memory_limit=?", [args.memory_limit])
            # Align omitted NULLS clauses with Paro/PostgreSQL before deriving
            # one shared peer-order contract from the query text.
            duck.execute("SET default_null_order='NULLS_LAST_ON_ASC_FIRST_ON_DESC'")

            def run_paro(query: str) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...]]:
                with paro.cursor(binary=args.paro_result_format == "binary") as cursor:
                    cursor.execute(query)
                    schema = paro_schema(cursor.description or ())
                    return cursor.fetchall(), schema

            def run_duckdb(query: str) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...]]:
                result = duck.execute(query)
                schema = duckdb_schema(result.description or ())
                return result.fetchall(), schema

            for query_number in range(args.start, args.end + 1):
                query_id = f"{query_number:02d}"
                query = (args.query_dir / f"{query_id}.sql").read_text(encoding="utf-8")
                result: dict[str, Any] = {"query": query_id}
                try:
                    duck_rows, duck_schema = run_duckdb(query)
                    paro_rows, actual_schema = run_paro(query)
                    assert_compatible_schema(actual_schema, duck_schema)
                    expected = canonicalize_rows(duck_rows, duck_schema)
                    actual = canonicalize_rows(paro_rows, actual_schema)
                    assert_same_multiset(actual, expected)
                    order_keys = parse_order_contract(query, duck_schema)
                    expected_order = assert_peer_order(expected, order_keys)
                    actual_order = assert_peer_order(actual, order_keys)
                    if actual_order != expected_order:
                        raise AssertionError("ordered key sequence differs across engines")
                    oracle_digest = multiset_digest(expected)

                    def validate_sample(
                        engine: str,
                        rows: list[tuple[Any, ...]],
                        sample_schema: tuple[ColumnContract, ...],
                    ) -> tuple[str, str | None]:
                        assert_compatible_schema(sample_schema, duck_schema)
                        normalized = canonicalize_rows(rows, sample_schema)
                        digest = multiset_digest(normalized)
                        if digest != oracle_digest:
                            raise AssertionError(
                                f"{engine} sample digest differs from the verified oracle"
                            )
                        order_digest = assert_peer_order(normalized, order_keys)
                        if order_digest != expected_order:
                            raise AssertionError(
                                f"{engine} sample ordered-key sequence differs from the oracle"
                            )
                        return digest, order_digest

                    for _ in range(args.warmups):
                        validate_sample("paro warmup", *run_paro(query))
                        validate_sample("duckdb warmup", *run_duckdb(query))

                    samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
                    sample_digests: dict[str, list[str]] = {"paro": [], "duckdb": []}
                    paro_ran_first: list[bool] = []
                    runners = {"paro": run_paro, "duckdb": run_duckdb}
                    for iteration in range(args.iterations):
                        order = ["paro", "duckdb"]
                        if (query_number + iteration) % 2:
                            order.reverse()
                        paro_ran_first.append(order[0] == "paro")
                        for engine in order:
                            rows, sample_schema, elapsed_ms = timed_fetch(
                                lambda engine=engine: runners[engine](query)
                            )
                            digest, _ = validate_sample(engine, rows, sample_schema)
                            samples[engine].append(elapsed_ms)
                            sample_digests[engine].append(digest)

                    paro_timing = timing_summary(samples["paro"])
                    duckdb_timing = timing_summary(samples["duckdb"])
                    crossover = paired_order_balanced_ratio(
                        samples["paro"], samples["duckdb"], paro_ran_first
                    )
                    ratio = crossover["ratio"]
                    confidence_high = crossover["paired_confidence_interval_95"][1]
                    result.update(
                        status="passed",
                        rows=len(paro_rows),
                        schema={
                            "paro": schema_report(actual_schema),
                            "duckdb": schema_report(duck_schema),
                        },
                        order_keys=[key.__dict__ for key in order_keys],
                        oracle_result_sha256=oracle_digest,
                        oracle_order_key_sha256=expected_order,
                        measured_sample_result_sha256=sample_digests,
                        verified_measured_samples={
                            "paro": len(sample_digests["paro"]),
                            "duckdb": len(sample_digests["duckdb"]),
                        },
                        paro=paro_timing,
                        duckdb=duckdb_timing,
                        crossover=crossover,
                        paro_over_duckdb=round(ratio, 6),
                        faster_than_duckdb=confidence_high < 1,
                    )
                except Exception as error:
                    failures += 1
                    result.update(status="failed", error=f"{type(error).__name__}: {error}")
                report["queries"].append(result)
                report["passed"] = len(report["queries"]) - failures
                report["failed"] = failures
                report["faster_than_duckdb"] = sum(
                    item.get("faster_than_duckdb", False) for item in report["queries"]
                )
                write_report(args.report, report)
                if result["status"] == "passed":
                    confidence = result["crossover"]["paired_confidence_interval_95"]
                    print(
                        f"TPC-DS {query_id}: Paro {result['paro']['median_ms']:.3f} ms, "
                        f"DuckDB {result['duckdb']['median_ms']:.3f} ms, "
                        f"ratio {result['paro_over_duckdb']:.3f}, "
                        f"95% CI [{confidence[0]:.3f}, {confidence[1]:.3f}]",
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
