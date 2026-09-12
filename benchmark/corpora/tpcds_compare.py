#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Run evidence-grade, fresh-process Paro/DuckDB TPC-DS comparisons."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import random
import statistics
import time
import traceback
from pathlib import Path
from typing import Any

import _duckdb
import duckdb
import psycopg
from psycopg import sql

from benchmark_evidence import (
    ImmutableDataSeed,
    STATEMENT_TRACE_SCHEMA_VERSION,
    isolated_paro_server,
    build_benchmark_server,
    content_digest,
    hierarchical_abba_ratio,
    parse_statement_trace_log,
    repository_identity,
    statement_fingerprint,
    tree_digest,
    validate_statement_trace,
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
from tpcds_setup import DECLARED_KEYS


TPCDS_TABLES = frozenset(DECLARED_KEYS)
CONSTRAINTS_SQL = """
SELECT table_name, constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY table_name, constraint_name
"""
CONSTRAINT_COLUMNS_SQL = """
SELECT table_name, constraint_name, column_name, ordinal_position
FROM information_schema.key_column_usage
ORDER BY table_name, constraint_name, ordinal_position
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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
    parser.add_argument("--warmups-per-process", type=int, default=1)
    parser.add_argument("--process-blocks", type=int, default=5)
    parser.add_argument("--diagnostic-process-blocks", type=int, default=1)
    parser.add_argument("--measurement-rounds-per-process", type=int, default=3)
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--build-jobs", type=int, default=4)
    parser.add_argument(
        "--metadata-track",
        choices=("none", "generator-declared"),
        required=True,
        help="optimizer-visible uniqueness metadata loaded into Paro",
    )
    parser.add_argument(
        "--paro-result-format", choices=("binary", "text"), default="binary"
    )
    parser.add_argument(
        "--diagnostic-strong-incumbent",
        action="store_true",
        help=(
            "run the diagnostic cohort with an independent seed Memo followed by "
            "a fresh proof Memo; never affects normal C1 samples"
        ),
    )
    parser.add_argument(
        "--strong-incumbent-c1",
        action="store_true",
        help=(
            "run normal fresh-process C1 samples through the explicit two-Memo "
            "SeedPlan experiment; source generation remains included in C1"
        ),
    )
    parser.add_argument(
        "--strong-incumbent-upper-bound",
        action="store_true",
        help=(
            "when the strong-incumbent experiment is enabled, install the "
            "re-priced SeedPlan as a destination upper bound"
        ),
    )
    parser.add_argument(
        "--strong-incumbent-logical-injection",
        action="store_true",
        help=(
            "when the strong-incumbent experiment is enabled, inject the "
            "source logical shell into the destination Memo"
        ),
    )
    args = parser.parse_args()
    if (args.strong_incumbent_upper_bound or args.strong_incumbent_logical_injection) and not (
        args.strong_incumbent_c1 or args.diagnostic_strong_incumbent
    ):
        parser.error(
            "strong-incumbent switches require --strong-incumbent-c1 or "
            "--diagnostic-strong-incumbent"
        )
    return args


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


def hierarchical_cold_ratio(
    blocks: list[dict[str, Any]], bootstrap_samples: int = 10_000
) -> dict[str, Any]:
    """Compare one cold observation per engine in each fresh process block.

    Cold samples are deliberately kept out of the warm ABBA distribution. A
    fresh process block is the resampling unit; there is no within-block IID
    resampling to pretend exists for a first statement.
    """
    if not blocks:
        raise ValueError("cold comparison has no process blocks")
    ratios = []
    for block in blocks:
        cold = block.get("cold_statement_ms") or {}
        try:
            paro = float(cold["paro"])
            duckdb = float(cold["duckdb"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "each process block must contain one cold sample per engine"
            ) from error
        if paro <= 0.0 or duckdb <= 0.0:
            raise ValueError("cold timings must be positive")
        ratios.append(paro / duckdb)

    ratio = statistics.geometric_mean(ratios)
    rng = random.Random(0)
    bootstrap = [
        statistics.geometric_mean(
            [ratios[rng.randrange(len(ratios))] for _ in ratios]
        )
        for _ in range(bootstrap_samples)
    ]
    bootstrap.sort()
    low = bootstrap[int(0.025 * (len(bootstrap) - 1))]
    high = bootstrap[int(0.975 * (len(bootstrap) - 1))]
    return {
        "ratio": round(ratio, 6),
        "hierarchical_confidence_interval_95": [round(low, 6), round(high, 6)],
        "process_block_ratios": [round(value, 6) for value in ratios],
        "process_blocks": len(ratios),
        "samples_per_engine": len(ratios),
        "bootstrap_samples": bootstrap_samples,
        "resampling_unit": "fresh_process_block_only",
    }


def schema_report(schema: tuple[ColumnContract, ...]) -> list[dict[str, str]]:
    return [
        {
            "name": column.name,
            "logical_type": column.logical_type,
            "engine_type": column.engine_type,
        }
        for column in schema
    ]


def _duckdb_worker(channel: Any, database: str, threads: int, memory_limit: str) -> None:
    connection = None
    try:
        connection = duckdb.connect(database, read_only=True)
        connection.execute(f"SET threads={threads}")
        connection.execute("SET memory_limit=?", [memory_limit])
        connection.execute("SET default_null_order='NULLS_LAST_ON_ASC_FIRST_ON_DESC'")
        channel.send({"status": "ready", "pid": os.getpid()})
        while True:
            request = channel.recv()
            if request["action"] == "close":
                return
            query = request["query"]
            started = time.perf_counter_ns()
            result = connection.execute(query)
            rows = result.fetchall()
            # Native result metadata is part of the engine-side timer. The
            # canonical Python schema conversion happens after the worker
            # reports elapsed_ms, just as it does for Paro.
            description = [(column[0], str(column[1])) for column in result.description or ()]
            elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
            channel.send(
                {
                    "status": "ok",
                    "rows": rows,
                    "description": description,
                    "elapsed_ms": elapsed_ms,
                }
            )
    except BaseException as error:
        channel.send(
            {
                "status": "error",
                "error": f"{type(error).__name__}: {error}",
                "traceback": traceback.format_exc(),
            }
        )
    finally:
        if connection is not None:
            connection.close()
        channel.close()


class DuckDBProcess:
    def __init__(self, database: Path, threads: int, memory_limit: str) -> None:
        context = multiprocessing.get_context("spawn")
        self._parent, child = context.Pipe()
        self._process = context.Process(
            target=_duckdb_worker,
            args=(child, str(database.resolve()), threads, memory_limit),
        )
        self._process.start()
        child.close()
        ready = self._parent.recv()
        if ready.get("status") != "ready":
            self.close()
            raise RuntimeError(ready.get("error", "DuckDB worker did not become ready"))
        self.identity = {
            "pid": int(ready["pid"]),
            "start_method": "spawn",
            "fresh_engine_process": True,
        }

    def execute(
        self, query: str
    ) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...], float]:
        self._parent.send({"action": "execute", "query": query})
        response = self._parent.recv()
        if response.get("status") != "ok":
            raise RuntimeError(
                f"DuckDB worker failed: {response.get('error')}\n{response.get('traceback', '')}"
            )
        return (
            response["rows"],
            duckdb_schema(response["description"]),
            float(response["elapsed_ms"]),
        )

    def close(self) -> None:
        if self._process.is_alive():
            try:
                self._parent.send({"action": "close"})
                self._process.join(timeout=10)
            except (BrokenPipeError, EOFError):
                pass
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=5)
        self._parent.close()

    def __enter__(self) -> "DuckDBProcess":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def configure_paro(connection: psycopg.Connection[Any], args: argparse.Namespace) -> None:
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


def run_paro_raw(
    connection: psycopg.Connection[Any], query: str, binary_result: bool
) -> tuple[list[tuple[Any, ...]], tuple[Any, ...]]:
    with connection.cursor(binary=binary_result) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows, tuple(cursor.description or ())


def collect_statement_cache_evidence(
    connection: psycopg.Connection[Any], query: str
) -> dict[str, Any]:
    """Read the lightweight miss side-channel after, never before, C1."""
    fingerprint = statement_fingerprint(query)
    prefix = f"statement_plan_cache/{fingerprint:016x}/"
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM paro_optimizers()")
            columns = [column.name for column in cursor.description or ()]
            rows = cursor.fetchall()
        indexes = {
            column.rsplit(".", 1)[-1]: index
            for index, column in enumerate(columns)
        }
        name_index = indexes["name"]
        kind_index = indexes["kind"]
        value_index = indexes["metric_value"]
        unit_index = indexes["metric_unit"]
        matches = [
            row
            for row in rows
            if str(row[kind_index]) == "evidence"
            and str(row[unit_index]) == "count"
            and str(row[name_index]).startswith(prefix)
        ]
        if len(matches) != 1:
            return {
                "status": "uncovered",
                "query_fingerprint": fingerprint,
                "reason": "post-timer cache decision was missing or ambiguous",
                "matching_rows": len(matches),
            }
        name = str(matches[0][name_index])
        occurrence = int(name.rsplit("/", 1)[1])
        cache_hit = int(matches[0][value_index]) == 1
        work_prefix = f"statement_compile_work/{fingerprint:016x}/{occurrence}/"
        compile_work = {
            str(row[name_index])[len(work_prefix):]: int(row[value_index])
            for row in rows
            if str(row[kind_index]) == "evidence"
            and str(row[name_index]).startswith(work_prefix)
        }
        return {
            "status": "verified",
            "query_fingerprint": fingerprint,
            "occurrence": occurrence,
            "cache_hit": cache_hit,
            "compile_work": compile_work,
            "source": "paro_optimizers_post_timer_side_channel",
        }
    except (IndexError, KeyError, TypeError, ValueError, psycopg.Error) as error:
        return {
            "status": "uncovered",
            "query_fingerprint": fingerprint,
            "reason": f"post-timer cache decision could not be read: {error}",
        }


def run_paro(
    connection: psycopg.Connection[Any], query: str, binary_result: bool
) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...]]:
    rows, description = run_paro_raw(connection, query, binary_result)
    return rows, paro_schema(description)


def timed_run_paro(
    connection: psycopg.Connection[Any], query: str, binary_result: bool
) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...], float]:
    started = time.perf_counter_ns()
    rows, description = run_paro_raw(connection, query, binary_result)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return rows, paro_schema(description), elapsed_ms


def metadata_inventory_from_rows(
    constraints: list[tuple[Any, ...]], columns: list[tuple[Any, ...]]
) -> list[dict[str, Any]]:
    column_map: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for table, constraint, column, ordinal in columns:
        table_name = str(table).lower()
        if table_name in TPCDS_TABLES:
            column_map.setdefault((table_name, str(constraint)), []).append(
                (int(ordinal), str(column).lower())
            )
    inventory = []
    for table, constraint, constraint_type in constraints:
        table_name = str(table).lower()
        if table_name not in TPCDS_TABLES:
            continue
        key_columns = tuple(
            column
            for _, column in sorted(column_map.get((table_name, str(constraint)), []))
        )
        if not key_columns:
            raise AssertionError(
                f"constraint {constraint!r} on {table_name} has no key-column metadata"
            )
        inventory.append(
            {
                "table": table_name,
                "kind": str(constraint_type).upper(),
                "columns": list(key_columns),
            }
        )
    return sorted(inventory, key=lambda item: (item["table"], item["columns"], item["kind"]))


def paro_metadata_inventory(connection: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(CONSTRAINTS_SQL)
        constraints = cursor.fetchall()
        cursor.execute(CONSTRAINT_COLUMNS_SQL)
        columns = cursor.fetchall()
    return metadata_inventory_from_rows(constraints, columns)


def duckdb_metadata_inventory(worker: DuckDBProcess) -> list[dict[str, Any]]:
    constraints, _, _ = worker.execute(CONSTRAINTS_SQL)
    columns, _, _ = worker.execute(CONSTRAINT_COLUMNS_SQL)
    return metadata_inventory_from_rows(constraints, columns)


def key_set(inventory: list[dict[str, Any]]) -> set[tuple[str, tuple[str, ...]]]:
    return {(item["table"], tuple(item["columns"])) for item in inventory}


def validate_metadata_track(
    requested: str,
    paro_inventory: list[dict[str, Any]],
    duckdb_inventory: list[dict[str, Any]],
) -> bool:
    paro_keys = key_set(paro_inventory)
    if requested == "none" and paro_keys:
        raise AssertionError(f"Paro metadata track is not empty: {sorted(paro_keys)}")
    if requested == "generator-declared":
        expected = {(table, columns) for table, columns in DECLARED_KEYS.items()}
        if paro_keys != expected:
            raise AssertionError(
                "Paro generator-declared key inventory differs from the setup contract"
            )
    return paro_keys == key_set(duckdb_inventory)


def open_paro_connection(args: argparse.Namespace) -> psycopg.Connection[Any]:
    host, port = args.listen.rsplit(":", 1)
    connection = psycopg.connect(
        f"host={host} port={port} dbname={args.database} user={args.user}",
        autocommit=True,
    )
    configure_paro(connection, args)
    return connection


def verify_measurement_inputs(repo_root: Path, binary: Path,
                              args: argparse.Namespace, report: dict[str, Any]) -> None:
    """Qualification requires the same source, binary, harness, SQL and data."""
    checks = {
        "source": repository_identity(repo_root) == report["source"] == report["build_attestation"]["source"],
        "binary": content_digest(binary) == report["build_attestation"]["binary_sha256"],
        "harness": all(content_digest(Path(item["path"])) == item["sha256"]
                       for item in report["harness"]["files"]),
        "SQL": tree_digest(args.query_dir, (".sql",)) == report["query_corpus_sha256"],
        "source data": tree_digest(args.dataset_source_dir) == report["dataset"]["source_sha256"],
        "Paro seed": tree_digest(args.server_data_dir) == report["dataset"]["paro_data_sha256"],
        "DuckDB data": content_digest(args.duckdb_database) == report["dataset"]["duckdb_sha256"],
    }
    changed = [name for name, current in checks.items() if not current]
    if changed:
        raise RuntimeError(f"measurement inputs changed: {', '.join(changed)}")


def main() -> int:
    args = parse_args()
    if not 1 <= args.start <= args.end <= 99:
        raise SystemExit("query range must satisfy 1 <= start <= end <= 99")
    if (
        args.warmups_per_process < 1
        or args.process_blocks < 2
        or args.diagnostic_process_blocks < 1
        or args.measurement_rounds_per_process < 1
    ):
        raise SystemExit(
            "warmups-per-process must be at least one so cached and cold-statement "
            "latencies have distinct, auditable scopes; process-blocks must be at least two; "
            "measurement-rounds-per-process must be at least one; diagnostic-process-blocks "
            "must be positive"
        )
    if args.bootstrap_samples < 100:
        raise SystemExit("bootstrap-samples must be at least 100")

    repo_root = Path(__file__).resolve().parents[2]
    server_binary, build = build_benchmark_server(repo_root, args.build_jobs)
    seed = ImmutableDataSeed.capture(args.server_data_dir)
    harness_files = [
        Path(__file__).resolve(),
        Path(__file__).with_name("benchmark_evidence.py").resolve(),
        Path(__file__).with_name("tpcds_result_contract.py").resolve(),
        Path(__file__).with_name("tpcds_setup.py").resolve(),
    ]
    report: dict[str, Any] = {
        "schema_version": 8,
        "corpus": "TPC-DS",
        "scale_factor": 1,
        "query_range": [args.start, args.end],
        "source": repository_identity(repo_root),
        "build_attestation": build,
        "query_corpus_sha256": tree_digest(args.query_dir, (".sql",)),
        "dataset": {
            "source_path": str(args.dataset_source_dir.resolve()),
            "source_sha256": tree_digest(args.dataset_source_dir),
            "schema_sql_sha256": content_digest(args.dataset_source_dir / "schema.sql"),
            "load_sql_sha256": content_digest(args.dataset_source_dir / "load.sql"),
            "paro_data_path": str(seed.path),
            "paro_data_sha256": seed.sha256,
            "duckdb_path": str(args.duckdb_database.resolve()),
            "duckdb_sha256": content_digest(args.duckdb_database),
            "requested_metadata_track": args.metadata_track,
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
            "warmups_per_process": args.warmups_per_process,
            "process_blocks": args.process_blocks,
            "diagnostic_process_blocks": args.diagnostic_process_blocks,
            "measurement_rounds_per_process": args.measurement_rounds_per_process,
            "samples_per_process_per_engine": args.measurement_rounds_per_process * 2,
            "samples_per_engine": (
                args.process_blocks * args.measurement_rounds_per_process * 2
            ),
            "optimizer_verify": True,
            "planning_dop": 1,
            "execution_dop": args.threads,
            "cohorts": {
                "normal": {
                    "purpose": "primary C1/W",
                    "statement_trace": False,
                    "statement_cache_evidence": True,
                    "allocation_profile": False,
                    "strong_incumbent_experiment": args.strong_incumbent_c1,
                    "strong_incumbent_provide_bound": (
                        args.strong_incumbent_c1
                        and args.strong_incumbent_upper_bound
                    ),
                    "strong_incumbent_inject_logical": (
                        args.strong_incumbent_c1
                        and args.strong_incumbent_logical_injection
                    ),
                },
                "diagnostic": {
                    "purpose": "same-operation phase attribution",
                    "statement_trace": True,
                    "statement_cache_evidence": True,
                    "excluded_from_c1": True,
                    "strong_incumbent_experiment": args.diagnostic_strong_incumbent,
                    "strong_incumbent_provide_bound": (
                        args.diagnostic_strong_incumbent
                        and args.strong_incumbent_upper_bound
                    ),
                    "strong_incumbent_inject_logical": (
                        args.diagnostic_strong_incumbent
                        and args.strong_incumbent_logical_injection
                    ),
                },
            },
            "resource_envelope": {
                "service_cpu_limit": args.threads,
                "memory_limit": args.memory_limit,
                "concurrent_target_statements": 1,
                "page_cache_policy": "private_copy_per_process; OS cache not flushed",
            },
            "latency_tracks": {
                "C0": {
                    "name": "first_result_from_process_start",
                    "status": "uncovered",
                    "auxiliary": "managed_server.startup_to_ready_ms",
                },
                "C1": {
                    "name": "cold_first_statement",
                    "timer": "client perf_counter_ns around execute/fetch/native result metadata",
                    "cohort": "normal_trace_off",
                    "primary_gate": True,
                },
                "C2": {
                    "name": "first_noncompile_path",
                    "timer": "same-operation interval union after compiler return",
                    "status": "diagnostic_only_until_interval_union_is_complete",
                    "diagnostic_only": True,
                },
                "W": {
                    "name": "steady_state",
                    "timer": "execute/fetch/native result metadata after cold sample and cache warmup",
                    "execution_quality_gate": True,
                },
            },
            "timing_scope": {
                "steady_state": "execute_fetch_and_result_metadata_after_instance_plan_cache_warmup",
                "cold_statement": "first_statement_parse_compile_admit_execute_fetch_and_native_result_metadata",
                "native_result_metadata": "included_in_each_engine_timer",
                "canonical_schema_conversion": "outside_each_engine_timer",
                "server_phase_trace": {
                    "schema_version": STATEMENT_TRACE_SCHEMA_VERSION,
                    "clock": "server_monotonic_relative",
                    "target": "paro::statement_trace",
                    "correlation": "process_id_session_id_operation_id_sample_id_sequence",
                    "client_timer": "perf_counter_ns",
                    "clock_subtraction": False,
                    "cohort": "diagnostic_only",
                },
            },
            "status_semantics": {
                "evidence": "trace/schema/identity/complete-result validation",
                "regression": "paired normal trace-off C1 comparison against declared baseline",
                "milestone": "pre-registered C1/quality/resource thresholds",
                "model": "G-Stats/G-Cost admitted only after declared calibration",
            },
            "validation_scope": "outside_timed_region_every_sample",
            "measurement_order": (
                "seeded_random_ABBA_per_round_within_fresh_process_block"
            ),
            "paro_input_isolation": "private_copy_per_process",
            "paro_result_format": args.paro_result_format,
            "random_seed": args.random_seed,
            "runtime_environment": {
                "RUST_LOG": os.environ.get("RUST_LOG"),
                "PARO_STATEMENT_CACHE_EVIDENCE": "1",
                "PARO_COMPILE_WORK_EVIDENCE": os.environ.get("PARO_COMPILE_WORK_EVIDENCE"),
                # Explicit diagnostic-only search deadline.  An absent value
                # means the production/default policy was used; keep this in
                # the report so a checkpoint run cannot be mistaken for a
                # full-search C1 sample.
                "PARO_DIAGNOSTIC_SEARCH_STOP_MS": os.environ.get(
                    "PARO_DIAGNOSTIC_SEARCH_STOP_MS"
                ),
                "PARO_QUALITY_POLICY_HANDOFF": os.environ.get(
                    "PARO_QUALITY_POLICY_HANDOFF"
                ),
                "PARO_CERTIFIED_GROUP_PRUNING": os.environ.get(
                    "PARO_CERTIFIED_GROUP_PRUNING"
                ),
                "PARO_DISABLE_PROTECTED_INCUMBENT": os.environ.get(
                    "PARO_DISABLE_PROTECTED_INCUMBENT"
                ),
                "PARO_EXPORT_STRONG_INCUMBENT": os.environ.get(
                    "PARO_EXPORT_STRONG_INCUMBENT"
                ),
                "PARO_STRONG_INCUMBENT_EXPERIMENT": os.environ.get(
                    "PARO_STRONG_INCUMBENT_EXPERIMENT"
                ),
                "PARO_STRONG_INCUMBENT_PROVIDE_BOUND": os.environ.get(
                    "PARO_STRONG_INCUMBENT_PROVIDE_BOUND"
                ),
                "PARO_STRONG_INCUMBENT_INJECT_LOGICAL": os.environ.get(
                    "PARO_STRONG_INCUMBENT_INJECT_LOGICAL"
                ),
            },
        },
        "model_gates": {
            "version": 1,
            "G-Stats": {
                "status": "registered_not_admitted",
                "scope": "declared semantic occurrences plus held-out cardinality families",
                "thresholds": {
                    "q_error_p95_max": 2.0,
                    "q_error_max": 4.0,
                    "conditional_domain_coverage_min": 1.0,
                },
            },
            "G-Cost": {
                "status": "registered_not_admitted",
                "scope": "fresh-process phase trace and candidate replay on the declared envelope",
                "thresholds": {
                    "phase_time_prediction_p95_ratio_max": 1.25,
                    "candidate_selection_loss_p95_ratio_max": 1.10,
                    "held_out_coverage_min": 1.0,
                },
            },
        },
        "manifest": {
            "version": 1,
            "family": "TPC-DS SF1",
            "held_out_required": True,
            "unsupported_scope": "queries outside the declared range or metadata track",
        },
        "validation": {
            "rows": "typed_full_multiset_digest_every_sample",
            "ordering": "explicit_result_keys_with_peer_group_semantics",
            "schema": "metadata_name_arity_exact_logical_type",
            "statistics": "hierarchical_bootstrap_process_block_then_sample",
            "optimizer_metadata": "live_information_schema_key_inventory",
        },
        "duckdb": {
            "version": duckdb.__version__,
            "python_extension": extension_digest(_duckdb),
            "execution_isolation": "spawned_fresh_process_per_ABBA_block",
        },
        "queries": [],
    }

    failures = 0
    binary_result = args.paro_result_format == "binary"
    for query_number in range(args.start, args.end + 1):
        query_id = f"{query_number:02d}"
        query = (args.query_dir / f"{query_id}.sql").read_text(encoding="utf-8")
        result: dict[str, Any] = {"query": query_id}
        try:
            oracle_log = args.report.with_suffix(f".q{query_id}.oracle.parod.log")
            oracle_server_context = isolated_paro_server(
                server_binary,
                seed,
                args.listen,
                oracle_log,
                max_memory=args.memory_limit,
                threads=args.threads,
                statement_trace=False,
                optimizer_environment={
                    "PARO_QUALITY_POLICY_HANDOFF": os.environ.get(
                        "PARO_QUALITY_POLICY_HANDOFF"
                    ),
                    "PARO_STRONG_INCUMBENT_EXPERIMENT": None,
                    "PARO_STRONG_INCUMBENT_PROVIDE_BOUND": None,
                    "PARO_STRONG_INCUMBENT_INJECT_LOGICAL": None,
                },
            )
            with oracle_server_context as oracle_server, DuckDBProcess(
                args.duckdb_database, args.threads, args.memory_limit
            ) as oracle_duck:
                oracle_paro = open_paro_connection(args)
                try:
                    duck_rows, duck_schema, _ = oracle_duck.execute(query)
                    paro_rows, actual_schema = run_paro(oracle_paro, query, binary_result)
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
                    paro_inventory = paro_metadata_inventory(oracle_paro)
                    duckdb_inventory = duckdb_metadata_inventory(oracle_duck)
                    metadata_symmetric = validate_metadata_track(
                        args.metadata_track, paro_inventory, duckdb_inventory
                    )
                    oracle_identity = {
                        "paro": oracle_server.identity(),
                        "duckdb": oracle_duck.identity,
                    }
                finally:
                    oracle_paro.close()

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

            samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
            cold_samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
            sample_digests: dict[str, list[str]] = {"paro": [], "duckdb": []}
            blocks: list[dict[str, Any]] = []
            rng = random.Random(args.random_seed + query_number * 1_000_003)
            for block_number in range(args.process_blocks):
                block_log = args.report.with_suffix(
                    f".q{query_id}.block{block_number:03d}.parod.log"
                )
                block_server_context = isolated_paro_server(
                    server_binary,
                    seed,
                    args.listen,
                    block_log,
                    max_memory=args.memory_limit,
                    threads=args.threads,
                    statement_trace=False,
                    cache_evidence=True,
                    optimizer_environment={
                        "PARO_QUALITY_POLICY_HANDOFF": os.environ.get(
                            "PARO_QUALITY_POLICY_HANDOFF"
                        ),
                        "PARO_STRONG_INCUMBENT_EXPERIMENT": (
                            "1" if args.strong_incumbent_c1 else None
                        ),
                        "PARO_STRONG_INCUMBENT_PROVIDE_BOUND": (
                            "1"
                            if args.strong_incumbent_c1
                            and args.strong_incumbent_upper_bound
                            else None
                        ),
                        "PARO_STRONG_INCUMBENT_INJECT_LOGICAL": (
                            "1"
                            if args.strong_incumbent_c1
                            and args.strong_incumbent_logical_injection
                            else None
                        ),
                    },
                )
                with block_server_context as block_server, DuckDBProcess(
                    args.duckdb_database, args.threads, args.memory_limit
                ) as duck_process:
                    paro = open_paro_connection(args)
                    try:
                        if paro_metadata_inventory(paro) != paro_inventory:
                            raise AssertionError("Paro metadata changed between process blocks")
                        if duckdb_metadata_inventory(duck_process) != duckdb_inventory:
                            raise AssertionError("DuckDB metadata changed between process blocks")

                        cold_order = ["paro", "duckdb"]
                        if rng.getrandbits(1):
                            cold_order.reverse()
                        cold_statement_ms: dict[str, float] = {}
                        cold_cache_evidence: dict[str, Any] = {
                            "status": "uncovered",
                            "reason": "Paro cold observation was not reached",
                        }
                        for engine in cold_order:
                            if engine == "paro":
                                rows, sample_schema, elapsed_ms = timed_run_paro(
                                    paro, query, binary_result
                                )
                            else:
                                rows, sample_schema, elapsed_ms = duck_process.execute(query)
                            validate_sample(f"{engine} cold statement", rows, sample_schema)
                            cold_samples[engine].append(elapsed_ms)
                            cold_statement_ms[engine] = round(elapsed_ms, 6)
                            if engine == "paro":
                                cold_cache_evidence = collect_statement_cache_evidence(
                                    paro, query
                                )

                        # The measured cold statement is also the first warmup.
                        # Additional warmups are deliberately outside both timed scopes.
                        for _ in range(args.warmups_per_process - 1):
                            validate_sample(
                                "paro warmup", *run_paro(paro, query, binary_result)
                            )
                            duck_rows, duck_schema_sample, _ = duck_process.execute(query)
                            validate_sample("duckdb warmup", duck_rows, duck_schema_sample)

                        round_orders = []
                        for _ in range(args.measurement_rounds_per_process):
                            if rng.getrandbits(1):
                                round_orders.append(
                                    ["paro", "duckdb", "duckdb", "paro"]
                                )
                            else:
                                round_orders.append(
                                    ["duckdb", "paro", "paro", "duckdb"]
                                )
                        block = {
                            "block": block_number,
                            "cohort": "normal",
                            "cold_order": cold_order,
                            "cold_statement_ms": cold_statement_ms,
                            "cold_miss_evidence": cold_cache_evidence,
                            "measurement_round_orders": round_orders,
                            "paro_ms": [],
                            "duckdb_ms": [],
                            "paro_server": block_server.identity(),
                            "duckdb_process": duck_process.identity,
                        }
                        for order in round_orders:
                            for engine in order:
                                if engine == "paro":
                                    rows, sample_schema, elapsed_ms = timed_run_paro(
                                        paro, query, binary_result
                                    )
                                else:
                                    rows, sample_schema, elapsed_ms = duck_process.execute(
                                        query
                                    )
                                digest, _ = validate_sample(engine, rows, sample_schema)
                                samples[engine].append(elapsed_ms)
                                sample_digests[engine].append(digest)
                                block[f"{engine}_ms"].append(round(elapsed_ms, 6))
                    finally:
                        paro.close()
                trace_events = parse_statement_trace_log(block_log)
                if trace_events:
                    raise AssertionError(
                        "normal C1 block emitted diagnostic trace despite trace-off configuration"
                    )
                block["statement_trace"] = {
                    "schema_version": STATEMENT_TRACE_SCHEMA_VERSION,
                    "enabled": False,
                    "verified_empty": True,
                }
                blocks.append(block)

            diagnostic_blocks: list[dict[str, Any]] = []
            for diagnostic_block_number in range(args.diagnostic_process_blocks):
                diagnostic_log = args.report.with_suffix(
                    f".q{query_id}.diagnostic{diagnostic_block_number:03d}.parod.log"
                )
                diagnostic_sample_id = (
                    f"q{query_id}.diagnostic.block{diagnostic_block_number}"
                )
                with isolated_paro_server(
                    server_binary,
                    seed,
                    args.listen,
                    diagnostic_log,
                    max_memory=args.memory_limit,
                    threads=args.threads,
                    statement_trace=True,
                    trace_sample_id=diagnostic_sample_id,
                    cache_evidence=True,
                    optimizer_environment={
                        "PARO_QUALITY_POLICY_HANDOFF": os.environ.get(
                            "PARO_QUALITY_POLICY_HANDOFF"
                        ),
                        "PARO_STRONG_INCUMBENT_EXPERIMENT": (
                            "1" if args.diagnostic_strong_incumbent else None
                        ),
                        "PARO_STRONG_INCUMBENT_PROVIDE_BOUND": (
                            "1"
                            if args.diagnostic_strong_incumbent
                            and args.strong_incumbent_upper_bound
                            else None
                        ),
                        "PARO_STRONG_INCUMBENT_INJECT_LOGICAL": (
                            "1"
                            if args.diagnostic_strong_incumbent
                            and args.strong_incumbent_logical_injection
                            else None
                        ),
                    },
                ) as diagnostic_server:
                    diagnostic_server_identity = diagnostic_server.identity()
                    diagnostic_paro = open_paro_connection(args)
                    try:
                        diagnostic_rows, diagnostic_schema, diagnostic_ms = timed_run_paro(
                            diagnostic_paro, query, binary_result
                        )
                        validate_sample(
                            "diagnostic Paro", diagnostic_rows, diagnostic_schema
                        )
                    finally:
                        diagnostic_paro.close()
                diagnostic_traces = parse_statement_trace_log(diagnostic_log)
                target_traces = [
                    trace
                    for trace in diagnostic_traces
                    if trace["query_fingerprint"] == statement_fingerprint(query)
                ]
                if len(target_traces) != 1:
                    raise AssertionError(
                        "diagnostic cohort must have exactly one target operation trace"
                    )
                validate_statement_trace(
                    target_traces[0],
                    expected_process_id=diagnostic_server_identity["pid"],
                    expected_sample_id=diagnostic_sample_id,
                    expected_query_fingerprint=statement_fingerprint(query),
                )
                diagnostic_blocks.append({
                    "block": diagnostic_block_number,
                    "cohort": "diagnostic",
                    "client_ms": round(diagnostic_ms, 6),
                    "paro_server": diagnostic_server_identity,
                    "statement_traces": diagnostic_traces,
                    "target_statement_traces": target_traces,
                })

            paro_timing = timing_summary(samples["paro"])
            duckdb_timing = timing_summary(samples["duckdb"])
            warm_crossover = hierarchical_abba_ratio(blocks, args.bootstrap_samples)
            warm_ratio = warm_crossover["ratio"]
            cold_crossover = hierarchical_cold_ratio(blocks, args.bootstrap_samples)
            cold_ratio = cold_crossover["ratio"]
            cold_confidence_high = cold_crossover[
                "hierarchical_confidence_interval_95"
            ][1]
            verify_measurement_inputs(repo_root, server_binary, args, report)
            trace_off_verified = all(
                item["statement_trace"]["verified_empty"] for item in blocks
            )
            cold_miss_evidence = {
                "status": (
                    "verified"
                    if all(
                        item.get("cold_miss_evidence", {}).get("status") == "verified"
                        and not item["cold_miss_evidence"].get("cache_hit", True)
                        for item in blocks
                    )
                    else "uncovered"
                ),
                "samples": [item.get("cold_miss_evidence") for item in blocks],
                "method": "post_timer_statement_plan_cache_side_channel",
            }
            cold_miss_verified = cold_miss_evidence["status"] == "verified"
            c1_p50_not_slower = (
                statistics.median(cold_samples["paro"])
                <= statistics.median(cold_samples["duckdb"])
            )
            evidence_qualifies = (
                metadata_symmetric
                and cold_confidence_high <= 1
                and c1_p50_not_slower
                and trace_off_verified
                and cold_miss_verified
            )
            result.update(
                status="passed",
                rows=len(paro_rows),
                schema={
                    "paro": schema_report(actual_schema),
                    "duckdb": schema_report(duck_schema),
                },
                order_keys=[key.__dict__ for key in order_keys],
                optimizer_metadata={
                    "requested_track": args.metadata_track,
                    "paro": paro_inventory,
                    "duckdb": duckdb_inventory,
                    "symmetric": metadata_symmetric,
                },
                oracle_processes=oracle_identity,
                oracle_result_sha256=oracle_digest,
                oracle_order_key_sha256=expected_order,
                measured_sample_result_sha256=sample_digests,
                verified_measured_samples={
                    "paro": len(sample_digests["paro"]),
                    "duckdb": len(sample_digests["duckdb"]),
                },
                process_blocks=blocks,
                diagnostic_cohort={
                    "process_blocks": diagnostic_blocks,
                    "trace_schema_version": STATEMENT_TRACE_SCHEMA_VERSION,
                    "excluded_from_c1": True,
                    "client_ms": timing_summary(
                        [item["client_ms"] for item in diagnostic_blocks]
                    ),
                },
                cold_statement={
                    "paro": timing_summary(cold_samples["paro"]),
                    "duckdb": timing_summary(cold_samples["duckdb"]),
                    "crossover": cold_crossover,
                    "cohort": "normal_trace_off",
                    "trace_enabled": False,
                    "trace_off_verified": trace_off_verified,
                    "trace_query_fingerprint": statement_fingerprint(query),
                    "cold_miss_evidence": cold_miss_evidence,
                },
                warmup_and_steady_state={
                    "paro": paro_timing,
                    "duckdb": duckdb_timing,
                    "crossover": warm_crossover,
                },
                paro=paro_timing,
                duckdb=duckdb_timing,
                crossover=warm_crossover,
                cold_crossover=cold_crossover,
                paro_over_duckdb=round(cold_ratio, 6),
                warm_paro_over_duckdb=round(warm_ratio, 6),
                faster_than_duckdb=evidence_qualifies,
                evidence_qualification=(
                    "qualified"
                    if evidence_qualifies
                    else "requires verified cold miss, C1 p50 non-regression and C1 CI upper bound at most one"
                ),
                evidence_status=("EvidenceValid" if cold_miss_verified else "EvidenceUncovered"),
                regression_status="RegressionCompared",
                milestone_status="MilestoneNotPassed",
                model_status="ModelNotAdmitted",
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
            c1_confidence = result["cold_crossover"][
                "hierarchical_confidence_interval_95"
            ]
            print(
                f"TPC-DS {query_id}: C1 Paro {result['cold_statement']['paro']['median_ms']:.3f} ms, "
                f"DuckDB {result['cold_statement']['duckdb']['median_ms']:.3f} ms, "
                f"ratio {result['paro_over_duckdb']:.3f}, "
                f"95% CI [{c1_confidence[0]:.3f}, {c1_confidence[1]:.3f}]; "
                f"W ratio {result['warm_paro_over_duckdb']:.3f}",
                flush=True,
            )
        else:
            print(f"TPC-DS {query_id}: failed: {result['error']}", flush=True)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
