#!/usr/bin/env python3
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
from typing import Any, Callable

import _duckdb
import duckdb
import psycopg
from psycopg import sql

from benchmark_evidence import (
    ManagedParoServer,
    build_benchmark_server,
    content_digest,
    hierarchical_abba_ratio,
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
            elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
            description = [(column[0], str(column[1])) for column in result.description or ()]
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


def run_paro(
    connection: psycopg.Connection[Any], query: str, binary_result: bool
) -> tuple[list[tuple[Any, ...]], tuple[ColumnContract, ...]]:
    with connection.cursor(binary=binary_result) as cursor:
        cursor.execute(query)
        schema = paro_schema(cursor.description or ())
        return cursor.fetchall(), schema


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


def main() -> int:
    args = parse_args()
    if not 1 <= args.start <= args.end <= 99:
        raise SystemExit("query range must satisfy 1 <= start <= end <= 99")
    if args.warmups_per_process < 0 or args.process_blocks < 2:
        raise SystemExit("warmups must be non-negative and process-blocks must be at least two")
    if args.bootstrap_samples < 100:
        raise SystemExit("bootstrap-samples must be at least 100")

    repo_root = Path(__file__).resolve().parents[2]
    server_binary, build = build_benchmark_server(repo_root, args.build_jobs)
    harness_files = [
        Path(__file__).resolve(),
        Path(__file__).with_name("benchmark_evidence.py").resolve(),
        Path(__file__).with_name("tpcds_result_contract.py").resolve(),
        Path(__file__).with_name("tpcds_setup.py").resolve(),
    ]
    report: dict[str, Any] = {
        "schema_version": 3,
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
            "paro_data_path": str(args.server_data_dir.resolve()),
            "paro_data_sha256_before_run": tree_digest(args.server_data_dir),
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
            "samples_per_engine": args.process_blocks * 2,
            "optimizer_verify": True,
            "timing_scope": "engine_execute_fetch_and_result_metadata",
            "validation_scope": "outside_timed_region_every_sample",
            "measurement_order": "seeded_random_ABBA_per_fresh_process_block",
            "paro_result_format": args.paro_result_format,
            "random_seed": args.random_seed,
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
            oracle_server = ManagedParoServer(
                server_binary, args.server_data_dir, args.listen, oracle_log
            )
            with oracle_server, DuckDBProcess(
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
            sample_digests: dict[str, list[str]] = {"paro": [], "duckdb": []}
            blocks: list[dict[str, Any]] = []
            rng = random.Random(args.random_seed + query_number * 1_000_003)
            for block_number in range(args.process_blocks):
                block_log = args.report.with_suffix(
                    f".q{query_id}.block{block_number:03d}.parod.log"
                )
                block_server = ManagedParoServer(
                    server_binary, args.server_data_dir, args.listen, block_log
                )
                with block_server, DuckDBProcess(
                    args.duckdb_database, args.threads, args.memory_limit
                ) as duck_process:
                    paro = open_paro_connection(args)
                    try:
                        if paro_metadata_inventory(paro) != paro_inventory:
                            raise AssertionError("Paro metadata changed between process blocks")
                        if duckdb_metadata_inventory(duck_process) != duckdb_inventory:
                            raise AssertionError("DuckDB metadata changed between process blocks")

                        for _ in range(args.warmups_per_process):
                            validate_sample(
                                "paro warmup", *run_paro(paro, query, binary_result)
                            )
                            duck_rows, duck_schema_sample, _ = duck_process.execute(query)
                            validate_sample("duckdb warmup", duck_rows, duck_schema_sample)

                        if rng.getrandbits(1):
                            order = ["paro", "duckdb", "duckdb", "paro"]
                        else:
                            order = ["duckdb", "paro", "paro", "duckdb"]
                        block = {
                            "block": block_number,
                            "order": order,
                            "paro_ms": [],
                            "duckdb_ms": [],
                            "paro_server": block_server.identity(),
                            "duckdb_process": duck_process.identity,
                        }
                        for engine in order:
                            if engine == "paro":
                                rows, sample_schema, elapsed_ms = timed_fetch(
                                    lambda: run_paro(paro, query, binary_result)
                                )
                            else:
                                rows, sample_schema, elapsed_ms = duck_process.execute(query)
                            digest, _ = validate_sample(engine, rows, sample_schema)
                            samples[engine].append(elapsed_ms)
                            sample_digests[engine].append(digest)
                            block[f"{engine}_ms"].append(round(elapsed_ms, 6))
                        blocks.append(block)
                    finally:
                        paro.close()

            paro_timing = timing_summary(samples["paro"])
            duckdb_timing = timing_summary(samples["duckdb"])
            crossover = hierarchical_abba_ratio(blocks, args.bootstrap_samples)
            ratio = crossover["ratio"]
            confidence_high = crossover["hierarchical_confidence_interval_95"][1]
            evidence_qualifies = metadata_symmetric and confidence_high < 1
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
                paro=paro_timing,
                duckdb=duckdb_timing,
                crossover=crossover,
                paro_over_duckdb=round(ratio, 6),
                faster_than_duckdb=evidence_qualifies,
                evidence_qualification=(
                    "qualified"
                    if evidence_qualifies
                    else "requires symmetric metadata and CI upper bound below one"
                ),
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
            confidence = result["crossover"]["hierarchical_confidence_interval_95"]
            print(
                f"TPC-DS {query_id}: Paro {result['paro']['median_ms']:.3f} ms, "
                f"DuckDB {result['duckdb']['median_ms']:.3f} ms, "
                f"ratio {result['paro_over_duckdb']:.3f}, "
                f"hierarchical 95% CI [{confidence[0]:.3f}, {confidence[1]:.3f}]",
                flush=True,
            )
        else:
            print(f"TPC-DS {query_id}: failed: {result['error']}", flush=True)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
