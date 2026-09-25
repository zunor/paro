#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Run evidence-grade, fresh-process Paro/DuckDB TPC-DS comparisons."""

from __future__ import annotations

import argparse
import json
import math
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
    CompileEvidenceCollector,
    isolated_paro_server,
    build_benchmark_server,
    content_digest,
    hierarchical_abba_ratio,
    repository_identity,
    statement_fingerprint,
    tree_digest,
)
from harness.receipt_contract import (
    EVIDENCE_SCHEMA_VERSION,
    ReceiptContractError,
    associate_typed_receipts,
    build_benchmark_cell_payload,
    uncovered_receipt,
    validate_compile_document,
)
from harness.run_output import CampaignOutput
from tpcds_result_contract import (
    RESULT_CONTRACT_VERSION,
    ColumnContract,
    assert_compatible_schema,
    assert_peer_order,
    assert_same_multiset,
    canonicalize_rows,
    duckdb_schema,
    multiset_digest,
    sequence_digest,
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
    parser.add_argument("--optimizer-search-policy", choices=("pipeline", "regional", "quality", "budgeted"), default="quality")
    parser.add_argument("--optimizer-aggregate-strategy", choices=("joint", "single_stage"), default="joint")
    parser.add_argument("--optimizer-verify", choices=("on", "off"), default="on")
    parser.add_argument("--disabled-optimizer-rules", default="", help=(
        "Comma-separated public rule names for a registered ablation; recorded as "
        "a search-domain change, not a production performance improvement."
    ))
    parser.add_argument("--listen", default="127.0.0.1:6432")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--duckdb-database", type=Path, required=True)
    parser.add_argument("--dataset-source-dir", type=Path, required=True)
    parser.add_argument("--query-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--pre-touch-sql", type=Path, help=(
        "One read-only SELECT executed and fully drained before the target in each "
        "engine process. Diagnostic only: target timing is not normal C1."
    ))
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--pre-touch-repetitions", type=int, choices=(1, 2), default=1,
                        help="Diagnostic only: record a second, warm pre-touch SELECT separately")
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


def read_pre_touch(path: Path | None, repetitions: int = 1) -> dict[str, Any] | None:
    if repetitions not in (1, 2) or (path is None and repetitions != 1):
        raise ValueError("one or two pre-touch executions require a pre-touch SQL file")
    if path is None:
        return None
    query = path.read_text(encoding="utf-8")
    statements = duckdb.extract_statements(query)
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise ValueError("pre-touch requires exactly one read-only SELECT")
    return {"path": str(path.resolve()), "sha256": content_digest(path),
            "sql": query, "query_fingerprint": statement_fingerprint(query),
            "repetitions": repetitions}


def is_target_cache_miss(evidence: dict[str, Any], query: str) -> bool:
    return (evidence.get("status") == "Verified"
            and evidence.get("compilation") == "Executed"
            and evidence.get("compile", {}).get("cache_hit") is False
            and evidence.get("query_fingerprint") == f"{statement_fingerprint(query):016x}")


def require_first_target_miss(evidence: dict[str, Any], query: str) -> None:
    if not is_target_cache_miss(evidence, query):
        raise AssertionError("pre-touch did not produce an exact target cache miss")


def _typed_optimizer_rows(
    connection: psycopg.Connection[Any],
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    """Read the versioned machine receipt channel without reconstructing enums.

    The old name/kind/metric-value side channel is intentionally not accepted
    here.  A receipt is usable only when the row identity and the embedded
    identity agree; malformed rows remain Uncovered evidence rather than being
    guessed into a target execution.
    """
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM paro_optimizers()")
        columns = [column.name.rsplit(".", 1)[-1] for column in cursor.description or ()]
        rows = cursor.fetchall()
    indexes = {column: index for index, column in enumerate(columns)}
    required = {"record_type", "record_id", "payload_json"}
    if not required.issubset(indexes):
        raise ValueError("typed receipt channel schema is missing required columns")
    decoded_rows: list[dict[str, Any]] = []
    for row in rows:
        record_type = row[indexes["record_type"]]
        if record_type not in {"statement_cache", "execution_receipt", "execution_work"}:
            continue
        payload = row[indexes["payload_json"]]
        if not payload:
            continue
        try:
            decoded = json.loads(str(payload))
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid typed receipt JSON: {error}") from error
        if not isinstance(decoded, dict):
            raise ValueError("typed receipt payload is not an object")
        record_id = row[indexes["record_id"]]
        if not isinstance(record_id, int) or isinstance(record_id, bool) or record_id < 0:
            raise ValueError("typed receipt row has an invalid record id")
        expected_id_key = {
            "statement_cache": "decision_id",
            "execution_receipt": "execution_id",
            "execution_work": "execution_id",
        }[record_type]
        if decoded.get(expected_id_key) != record_id:
            raise ValueError(f"typed {record_type} row identity does not match payload")
        decoded_rows.append({"record_type": record_type, "record_id": record_id, "payload": decoded})
    return indexes, decoded_rows


def snapshot_execution_ids(connection: psycopg.Connection[Any]) -> set[int]:
    """Capture the exact receipt boundary before one target execution."""
    _, rows = _typed_optimizer_rows(connection)
    return {
        row["record_id"]
        for row in rows
        if row["record_type"] == "execution_receipt"
    }


def collect_pre_touch(paro: Any, duck: Any, spec: dict[str, Any] | None,
                      target: str, binary: bool) -> dict[str, Any] | None:
    if spec is None:
        return None
    if spec["query_fingerprint"] == statement_fingerprint(target):
        raise ValueError("pre-touch must not share the target fingerprint")
    started = time.perf_counter_ns()
    records = {}
    schemas = {}
    normalized = {}
    for engine in ("paro", "duckdb") if duck is not None else ("paro",):
        before_execution_ids = (
            snapshot_execution_ids(paro)
            if engine == "paro" and paro is not None
            else None
        )
        rows, schema, elapsed = (timed_run_paro(paro, spec["sql"], binary)
                                 if engine == "paro" else duck.execute(spec["sql"]))
        schemas[engine] = schema
        normalized[engine] = canonicalize_rows(rows, schema)
        records[engine] = {"execute_fetch_ms": elapsed, "rows": len(rows),
                           "schema": schema_report(schema),
                           "result_sha256": multiset_digest(normalized[engine])}
        if engine == "paro":
            # Pre-touch is diagnostic.  Keep its receipt association exact, but
            # do not attempt to infer it after the fact from occurrence.
            records[engine]["cache_evidence"] = collect_statement_cache_evidence(
                paro, spec["sql"], before_execution_ids=before_execution_ids
            )
        if spec.get("repetitions", 1) == 2:
            before_execution_ids = (
                snapshot_execution_ids(paro)
                if engine == "paro" and paro is not None
                else None
            )
            warm_rows, warm_schema, warm_elapsed = (
                timed_run_paro(paro, spec["sql"], binary)
                if engine == "paro" else duck.execute(spec["sql"]))
            assert_compatible_schema(schema, warm_schema)
            warm_normalized = canonicalize_rows(warm_rows, warm_schema)
            assert_same_multiset(normalized[engine], warm_normalized)
            records[engine]["second_execution"] = {
                "execute_fetch_ms": warm_elapsed, "rows": len(warm_rows),
                "schema": schema_report(warm_schema),
                "result_sha256": multiset_digest(warm_normalized)}
            if engine == "paro":
                records[engine]["second_execution"]["cache_evidence"] = (
                    collect_statement_cache_evidence(
                        paro, spec["sql"], before_execution_ids=before_execution_ids
                    ))
    if duck is not None:
        assert_compatible_schema(schemas["paro"], schemas["duckdb"], query=spec["sql"])
        assert_same_multiset(normalized["paro"], normalized["duckdb"])
    return {"query_fingerprint": spec["query_fingerprint"], "engines": records,
            "preparation_wall_ms": (time.perf_counter_ns()-started)/1e6,
            "includes_evidence_and_validation": True, "excluded_from_target_timer": True,
            "target_is_normal_c1": False}


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


def corpus_impact_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Bounded within-campaign triage, never a pooled performance gate.

    Sum-of-medians is an equal-frequency prioritization proxy, not elapsed
    campaign time. Failed/uncovered cases have unknown weight and stay visible;
    they are not silently assigned zero or included in the denominator.
    """
    if len(results) > 99 or len({r["query"] for r in results}) != len(results):
        raise ValueError("corpus impact requires at most 99 distinct query cases")
    measured, uncovered = [], []
    for result in results:
        key = result["query"]
        if result.get("status") != "passed":
            uncovered.append({"query": key, "status": result.get("status", "Uncovered")})
            continue
        warm = result.get("warmup_and_steady_state", {})
        paro = warm.get("paro", {}).get("median_ms")
        duck = warm.get("duckdb", {}).get("median_ms")
        if not all(type(x) in (int, float) and math.isfinite(x) and x > 0 for x in (paro, duck)):
            uncovered.append({"query": key, "status": "Uncovered"})
            continue
        measured.append({
            "query": key, "paro_warm_median_ms": paro, "duckdb_warm_median_ms": duck,
            "median_ratio": paro / duck,
            "excess_warm_median_ms": max(0.0, paro - duck),
            # Selection for a future, separately identified diagnostic cohort.
            # This does not assert that an execution profile was collected.
            "execution_diagnosis_recommended": paro / duck > 3.0,
        })
    total = sum(row["paro_warm_median_ms"] for row in measured)
    for row in measured:
        row["measured_warm_share"] = row["paro_warm_median_ms"] / total
    measured.sort(key=lambda row: (-row["excess_warm_median_ms"], row["query"]))
    slowest = sorted(measured, key=lambda row: -row["paro_warm_median_ms"])[:5]
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "CorpusImpactTriage",
        "scope": "single campaign; equal query frequency; not a performance gate",
        "complete_measured_coverage": not uncovered and bool(measured),
        "registered_results": len(results), "measured_queries": len(measured),
        "sum_of_measured_warm_medians_ms": total if measured else None,
        "top_five_measured_warm_share": (
            sum(row["paro_warm_median_ms"] for row in slowest) / total if measured else None
        ),
        "ranked_by_excess_warm_ms": measured, "uncovered": uncovered,
        "evidence": "samples, failures and receipts remain in their registered cells",
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
        try:
            ready = self._parent.recv()
        except BaseException:
            # Construction can be cancelled before __enter__ owns the worker.
            # Do not leave multiprocessing's exit hook waiting on that child.
            self.close()
            raise
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
        cursor.execute(sql.SQL("SET optimizer_verify = {}").format(sql.Literal(args.optimizer_verify == "on")))
        cursor.execute(sql.SQL("SET optimizer_search_policy = {}").format(sql.Literal(args.optimizer_search_policy)))
        cursor.execute(sql.SQL("SET optimizer_aggregate_strategy = {}").format(sql.Literal(args.optimizer_aggregate_strategy)))
        cursor.execute(sql.SQL("SET disabled_optimizer_rules = {}").format(sql.Literal(args.disabled_optimizer_rules)))
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
    connection: psycopg.Connection[Any],
    query: str,
    *,
    before_execution_ids: set[int] | None = None,
) -> dict[str, Any]:
    """Associate one target execution with its exact statement decision.

    The boundary is mandatory.  A post-statement scan can itself publish a
    receipt, so selecting the newest row or a cache occurrence is not a valid
    association strategy.
    """
    fingerprint = statement_fingerprint(query)
    if before_execution_ids is None:
        return {
            **uncovered_receipt("target execution boundary was not captured"),
            "query_fingerprint": fingerprint,
        }
    try:
        _, rows = _typed_optimizer_rows(connection)
        decisions = {
            row["record_id"]: row["payload"]
            for row in rows
            if row["record_type"] == "statement_cache"
        }
        executions = {
            row["record_id"]: row["payload"]
            for row in rows
            if row["record_type"] == "execution_receipt"
        }
        works = {
            row["record_id"]: row["payload"]
            for row in rows
            if row["record_type"] == "execution_work"
        }
        association = associate_typed_receipts(
            decisions,
            executions,
            before_execution_ids=before_execution_ids,
            query_fingerprint=fingerprint,
        )
        if association.get("status") == "Verified":
            association["execution_work"] = works.get(
                association["execution_id"], {}
            )
            association["source"] = "paro_optimizers_typed_receipt_channel"
        return association
    except (IndexError, KeyError, TypeError, ValueError, psycopg.Error) as error:
        return {
            "status": "Uncovered",
            "query_fingerprint": fingerprint,
            "reason": f"typed receipt could not be read: {error}",
        }


def execution_work_from_rows(
    rows: list[Any], indexes: dict[str, int], fingerprint: int,
    *, before_execution_ids: set[int] | None = None,
) -> dict[str, Any]:
    """Compatibility-free typed test helper; it never chooses the latest ID."""
    if before_execution_ids is None:
        return {}
    records: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        if row[indexes["record_type"]] != "execution_work":
            continue
        payload = row[indexes["payload_json"]]
        if not payload:
            continue
        decoded = json.loads(str(payload))
        execution_id = row[indexes["record_id"]]
        if (
            isinstance(decoded, dict)
            and decoded.get("execution_id") == execution_id
            and decoded.get("query_fingerprint") == fingerprint
            and execution_id not in before_execution_ids
        ):
            records.append((execution_id, decoded))
    if len(records) != 1:
        return {}
    execution_id, record = records[0]
    return {
        "query_fingerprint": fingerprint,
        "execution_id": execution_id,
        "metrics": record.get("metrics", {}),
    }


def collect_execution_work(
    connection: psycopg.Connection[Any],
    query: str,
    *,
    before_execution_ids: set[int] | None = None,
) -> dict[str, Any]:
    if before_execution_ids is None:
        return {
            "status": "Uncovered",
            "query_fingerprint": statement_fingerprint(query),
            "reason": "target execution boundary was not captured",
        }
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM paro_optimizers()")
        columns = {
            column.name.rsplit(".", 1)[-1]: i
            for i, column in enumerate(cursor.description or ())
        }
        rows = cursor.fetchall()
    return execution_work_from_rows(
        rows, columns, statement_fingerprint(query),
        before_execution_ids=before_execution_ids,
    )


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


def normal_cell_evidence(result: dict[str, Any], sample_count: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Store each cold/warm receipt once, referenced by its block coordinate."""
    payload = dict(result)
    receipts = []
    blocks = []
    for block in result.get("process_blocks", []):
        compact = dict(block)
        compact["cold_receipt_index"] = len(receipts)
        receipts.append(compact.pop("cold_miss_evidence"))
        warm = compact.pop("paro_receipt_associations")
        compact["warm_receipt_indices"] = list(range(len(receipts), len(receipts) + len(warm)))
        receipts.extend(warm)
        blocks.append(compact)
    if len(receipts) > sample_count:
        raise ValueError("collected receipts exceed registered normal samples")
    receipts.extend(uncovered_receipt(result.get("error", "sample was not collected"))
                    for _ in range(sample_count - len(receipts)))
    payload["process_blocks"] = blocks
    cold = dict(payload.get("cold_statement", {}))
    coverage = dict(cold.get("normal_receipt_coverage", {}))
    coverage.pop("associations", None)
    cold["normal_receipt_coverage"] = coverage
    miss = dict(cold.get("cold_miss_evidence", {}))
    miss.pop("samples", None)
    cold["cold_miss_evidence"] = miss
    payload["cold_statement"] = cold
    # Aliases of warmup_and_steady_state/cold_statement, not distinct samples.
    for alias in ("paro", "duckdb", "crossover", "cold_crossover", "diagnostic_cohort"):
        payload.pop(alias, None)
    return payload, receipts


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
    if report.get("pre_touch"):
        spec = report["pre_touch"]
        checks["pre-touch SQL"] = content_digest(Path(spec["path"])) == spec["sha256"]
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
    pre_touch = read_pre_touch(args.pre_touch_sql, args.pre_touch_repetitions)
    server_binary, build = build_benchmark_server(repo_root, args.build_jobs)
    seed = ImmutableDataSeed.capture(args.server_data_dir)
    harness_files = [
        Path(__file__).resolve(),
        Path(__file__).with_name("benchmark_evidence.py").resolve(),
        Path(__file__).with_name("tpcds_result_contract.py").resolve(),
        Path(__file__).with_name("tpcds_setup.py").resolve(),
    ]
    report: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "compile_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "pre_touch": pre_touch,
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
            "compile_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
            "result_contract_version": RESULT_CONTRACT_VERSION,
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
            "optimizer_verify": args.optimizer_verify == "on",
            "optimizer_search_policy": args.optimizer_search_policy,
            "optimizer_aggregate_strategy": args.optimizer_aggregate_strategy,
            "disabled_optimizer_rules": args.disabled_optimizer_rules,
            "planning_dop": 1,
            "execution_dop": args.threads,
            "cohorts": {
                "normal": {
                    "purpose": "diagnostic pre-touched target/W" if pre_touch else "primary C1/W",
                    "trace_mode": "off",
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
                    "trace_mode": "off",
                    "compile_document": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)",
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
                    "status": "Uncovered",
                    "auxiliary": "managed_server.startup_to_ready_ms",
                },
                "C1": {
                    "name": "cold_first_statement",
                    "timer": "client perf_counter_ns around execute/fetch/native result metadata",
                    "cohort": "pre_touch_diagnostic_trace_off" if pre_touch else "normal_trace_off",
                    "primary_gate": pre_touch is None,
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
                "compile_document": {
                    "statement": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)",
                    "producer": "server_typed_compile_document",
                    "normal_timing": "trace_off; no auxiliary compile request",
                    "diagnostic_timing": "excluded_from_c1",
                },
            },
            "status_semantics": {
                "evidence": "typed compile/admission receipt, schema identity and complete-result validation",
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
                "PARO_COLD_WORK_EVIDENCE": os.environ.get("PARO_COLD_WORK_EVIDENCE"),
                "PARO_DIAGNOSTIC_STREAM_SEQUENTIAL": os.environ.get("PARO_DIAGNOSTIC_STREAM_SEQUENTIAL"),
                # Explicit diagnostic-only search deadline.  An absent value
                # means the production/default policy was used; keep this in
                # the report so a checkpoint run cannot be mistaken for a
                # full-search C1 sample.
                "PARO_DIAGNOSTIC_SEARCH_STOP_MS": os.environ.get(
                    "PARO_DIAGNOSTIC_SEARCH_STOP_MS"
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

    query_cases = [f"{number:02d}" for number in range(args.start, args.end + 1)]
    normal_rows = args.process_blocks * (1 + args.measurement_rounds_per_process * 2)
    output = CampaignOutput.create(
        args.report,
        source_id="tpcds_compare",
        cells=[
            {
                "query_case": query_id,
                "arm_id": arm,
                "query_cases": 1,
                "sample_rows": normal_rows if arm == "normal" else args.diagnostic_process_blocks,
                "product_receipts": normal_rows if arm == "normal" else args.diagnostic_process_blocks,
                "summary_captures": (
                    args.diagnostic_process_blocks if arm == "diagnostic" else 0
                ),
            }
            for query_id in query_cases
            for arm in ("normal", "diagnostic")
        ],
    )

    output.control.write_json("inputs.json", {
        key: value for key, value in report.items() if key != "queries"
    })
    failures = 0
    output_errors: dict[tuple[str, str], str] = {}
    binary_result = args.paro_result_format == "binary"
    for query_number in range(args.start, args.end + 1):
        query_id = f"{query_number:02d}"
        query = (args.query_dir / f"{query_id}.sql").read_text(encoding="utf-8")
        result: dict[str, Any] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "query": query_id,
        }
        try:
            oracle_server_context = isolated_paro_server(
                server_binary,
                seed,
                args.listen,
                None,
                max_memory=args.memory_limit,
                threads=args.threads,
                statement_trace=False,
                optimizer_environment={
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
                    from bound_result_contract import BoundResult, CATALOG_SQL, catalog_from_rows, order_values
                    catalog_rows, _, _ = oracle_duck.execute(CATALOG_SQL)
                    with duckdb.connect() as output_parser:
                        bound_result = BoundResult(query, output_parser, catalog_from_rows(catalog_rows))
                        bound_result.check_identity(actual_schema, "paro")
                        bound_result.check_identity(duck_schema, "duckdb")
                        bound_result.check_types(actual_schema, duck_schema)
                    expected = bound_result.canonical_rows(duck_rows, duck_schema, "duckdb")
                    actual = bound_result.canonical_rows(paro_rows, actual_schema, "paro")
                    assert_same_multiset(actual, expected)
                    order_keys = bound_result.bind_order(actual_schema, "paro")
                    duck_order_keys = bound_result.bind_order(duck_schema, "duckdb")
                    expected_order = order_values(expected, duck_order_keys)
                    actual_order = order_values(actual, order_keys)
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
                assert_compatible_schema(sample_schema, actual_schema if engine == "paro" else duck_schema)
                normalized = bound_result.canonical_rows(rows, sample_schema, engine)
                assert_same_multiset(normalized, expected)
                digest = multiset_digest(normalized)
                actual_keys = order_values(normalized, order_keys if engine == "paro" else duck_order_keys)
                if actual_keys != expected_order:
                    raise AssertionError(
                        f"{engine} sample ordered-key sequence differs from the oracle"
                    )
                order_digest = sequence_digest(actual_keys) if order_keys else None
                return digest, order_digest

            samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
            cold_samples: dict[str, list[float]] = {"paro": [], "duckdb": []}
            sample_digests: dict[str, list[str]] = {"paro": [], "duckdb": []}
            blocks: list[dict[str, Any]] = []
            result["process_blocks"] = blocks
            rng = random.Random(args.random_seed + query_number * 1_000_003)
            for block_number in range(args.process_blocks):
                block_server_context = isolated_paro_server(
                    server_binary,
                    seed,
                    args.listen,
                    None,
                    max_memory=args.memory_limit,
                    threads=args.threads,
                    statement_trace=False,
                    cache_evidence=True,
                    optimizer_environment={
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

                        preparation = collect_pre_touch(paro, duck_process, pre_touch, query, binary_result)

                        cold_order = ["paro", "duckdb"]
                        if rng.getrandbits(1):
                            cold_order.reverse()
                        cold_statement_ms: dict[str, float] = {}
                        cold_cache_evidence: dict[str, Any] = {
                            "status": "Uncovered",
                            "reason": "Paro cold observation was not reached",
                        }
                        for engine in cold_order:
                            if engine == "paro":
                                cold_before_execution_ids = snapshot_execution_ids(paro)
                                rows, sample_schema, elapsed_ms = timed_run_paro(
                                    paro, query, binary_result
                                )
                            else:
                                rows, sample_schema, elapsed_ms = duck_process.execute(query)
                            validate_sample(engine, rows, sample_schema)
                            cold_samples[engine].append(elapsed_ms)
                            cold_statement_ms[engine] = round(elapsed_ms, 6)
                            if engine == "paro":
                                cold_cache_evidence = collect_statement_cache_evidence(
                                    paro, query,
                                    before_execution_ids=cold_before_execution_ids,
                                )
                                if pre_touch:
                                    require_first_target_miss(cold_cache_evidence, query)

                        # The measured cold statement is also the first warmup.
                        # Additional warmups are deliberately outside both timed scopes.
                        for _ in range(args.warmups_per_process - 1):
                            validate_sample(
                                "paro", *run_paro(paro, query, binary_result)
                            )
                            duck_rows, duck_schema_sample, _ = duck_process.execute(query)
                            validate_sample("duckdb", duck_rows, duck_schema_sample)

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
                            "cohort": "pre_touch_diagnostic" if pre_touch else "normal",
                            "trace_mode": "off",
                            "pre_touch": preparation,
                            "cold_order": cold_order,
                            "cold_statement_ms": cold_statement_ms,
                            "cold_miss_evidence": cold_cache_evidence,
                            "measurement_round_orders": round_orders,
                            "paro_ms": [],
                            "paro_receipt_associations": [
                                uncovered_receipt("registered warm sample has not completed")
                                for _ in range(args.measurement_rounds_per_process * 2)
                            ],
                            "paro_execution_work": [],
                            "duckdb_ms": [],
                            "paro_server": block_server.identity(),
                            "duckdb_process": duck_process.identity,
                        }
                        blocks.append(block)
                        warm_receipt_index = 0
                        for order in round_orders:
                            for engine in order:
                                if engine == "paro":
                                    before_execution_ids = snapshot_execution_ids(paro)
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
                                if engine == "paro":
                                    block["paro_receipt_associations"][warm_receipt_index] = collect_statement_cache_evidence(
                                            paro,
                                            query,
                                            before_execution_ids=before_execution_ids,
                                    )
                                    warm_receipt_index += 1
                                    if os.environ.get("PARO_COLD_WORK_EVIDENCE") == "1":
                                        block["paro_execution_work"].append(
                                            collect_execution_work(
                                                paro,
                                                query,
                                                before_execution_ids=before_execution_ids,
                                            )
                                        )
                    finally:
                        paro.close()
                block["compile_document"] = {
                    "status": "Uncovered",
                    "reason": (
                        "normal timing uses the production statement receipt; "
                        "EXPLAIN (COMPILE) is diagnostic-only and would change C1"
                    ),
                }

            diagnostic_blocks: list[dict[str, Any]] = []
            for diagnostic_block_number in range(args.diagnostic_process_blocks):
                with isolated_paro_server(
                    server_binary,
                    seed,
                    args.listen,
                    None,
                    max_memory=args.memory_limit,
                    threads=args.threads,
                    statement_trace=False,
                    cache_evidence=True,
                    optimizer_environment={
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
                        diagnostic_preparation = collect_pre_touch(
                            diagnostic_paro, None, pre_touch, query, binary_result)
                        diagnostic_compile_raw, diagnostic_compile_document = CompileEvidenceCollector(
                            diagnostic_paro
                        ).capture(query, detail=True)
                        try:
                            validate_compile_document(diagnostic_compile_document)
                        except ReceiptContractError as error:
                            raise AssertionError(
                                f"diagnostic Compile Evidence failed validation: {error}"
                            ) from error
                        diagnostic_before_execution_ids = snapshot_execution_ids(diagnostic_paro)
                        diagnostic_rows, diagnostic_schema, diagnostic_ms = timed_run_paro(
                            diagnostic_paro, query, binary_result
                        )
                        validate_sample(
                            "paro", diagnostic_rows, diagnostic_schema
                        )
                        if pre_touch:
                            require_first_target_miss(
                                collect_statement_cache_evidence(
                                    diagnostic_paro,
                                    query,
                                    before_execution_ids=diagnostic_before_execution_ids,
                                ),
                                query,
                            )
                    finally:
                        diagnostic_paro.close()
                diagnostic_blocks.append({
                    "block": diagnostic_block_number,
                    "cohort": "diagnostic",
                    "pre_touch": diagnostic_preparation,
                    "client_ms": round(diagnostic_ms, 6),
                    "paro_server": diagnostic_server_identity,
                    "compile_document": diagnostic_compile_document,
                    "compile_document_raw": diagnostic_compile_raw,
                })

            # The typed EXPLAIN document is an immutable producer capture.
            # Store it once under the diagnostic cell and leave only a bounded
            # reference in the cell payload; embedding the raw document in
            # diagnostic_cohort would create a second evidence owner.
            for item in diagnostic_blocks:
                raw_document = item.pop("compile_document_raw", None)
                document = item.get("compile_document")
                if not isinstance(raw_document, str) or not isinstance(document, dict):
                    raise RuntimeError("diagnostic compile capture is incomplete")
                capture_path = output.publish_capture_text(
                    query_case=query_id,
                    arm_id="diagnostic",
                    name=f"block-{item['block']:04d}.json",
                    text=raw_document,
                )
                item["compile_document"] = {
                    "status": "Captured",
                    "path": capture_path.relative_to(output.run.root).as_posix(),
                    "sha256": content_digest(capture_path),
                    "schema_version": document.get("schema_version"),
                }

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
            normal_observation_verified = all(
                item.get("trace_mode") == "off"
                and item.get("compile_document", {}).get("status") == "Uncovered"
                and bool(item.get("compile_document", {}).get("reason"))
                for item in blocks
            )
            normal_receipt_associations = [
                receipt
                for item in blocks
                for receipt in item.get("paro_receipt_associations", [])
            ]
            normal_receipt_coverage = {
                "status": (
                    "Verified"
                    if normal_receipt_associations
                    and all(receipt.get("status") == "Verified"
                            for receipt in normal_receipt_associations)
                    else "Uncovered"
                ),
                "sample_count": len(normal_receipt_associations),
                "verified_count": sum(
                    receipt.get("status") == "Verified"
                    for receipt in normal_receipt_associations
                ),
                "associations": normal_receipt_associations,
                "method": "post_timer_statement_decision_and_execution_id",
            }
            cold_miss_evidence = {
                "status": (
                    "Verified"
                    if all(
                        is_target_cache_miss(item.get("cold_miss_evidence", {}), query)
                        for item in blocks
                    )
                    else "Uncovered"
                ),
                "samples": [item.get("cold_miss_evidence") for item in blocks],
                "method": "post_timer_typed_statement_receipt_channel",
            }
            cold_miss_verified = cold_miss_evidence["status"] == "Verified"
            c1_p50_not_slower = (
                statistics.median(cold_samples["paro"])
                <= statistics.median(cold_samples["duckdb"])
            )
            evidence_qualifies = (
                pre_touch is None
                and metadata_symmetric
                and cold_confidence_high <= 1
                and c1_p50_not_slower
                and normal_observation_verified
                and cold_miss_verified
                and normal_receipt_coverage["status"] == "Verified"
            )
            result.update(
                status="passed",
                rows=len(paro_rows),
                schema={
                    "paro": schema_report(actual_schema),
                    "duckdb": schema_report(duck_schema),
                },
                order_keys=[{"expression": repr(expr), "descending": desc, "nulls": nulls}
                            for expr, desc, nulls in order_keys],
                optimizer_metadata={
                    "requested_track": args.metadata_track,
                    "paro": paro_inventory,
                    "duckdb": duckdb_inventory,
                    "symmetric": metadata_symmetric,
                },
                oracle_processes=oracle_identity,
                oracle_result_sha256=oracle_digest,
                oracle_order_key_sha256=sequence_digest(expected_order) if order_keys else None,
                measured_sample_result_sha256=sample_digests,
                verified_measured_samples={
                    "paro": len(sample_digests["paro"]),
                    "duckdb": len(sample_digests["duckdb"]),
                },
                process_blocks=blocks,
                diagnostic_cohort={
                    "process_blocks": diagnostic_blocks,
                    "compile_document_schema_version": 3,
                    "excluded_from_c1": True,
                    "client_ms": timing_summary(
                        [item["client_ms"] for item in diagnostic_blocks]
                    ),
                },
                cold_statement={
                    "paro": timing_summary(cold_samples["paro"]),
                    "duckdb": timing_summary(cold_samples["duckdb"]),
                    "crossover": cold_crossover,
                    "cohort": "pre_touch_diagnostic_compile" if pre_touch else "normal_trace_off",
                    "primary_gate_eligible": pre_touch is None,
                    "trace_mode": "off",
                    "normal_observation_verified": normal_observation_verified,
                    "normal_receipt_coverage": normal_receipt_coverage,
                    "compile_document": "normal target uses production receipt channel",
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
                    "diagnostic pre-touch target is ineligible for C1/parity"
                    if pre_touch else "qualified"
                    if evidence_qualifies
                    else "requires verified cold miss, C1 p50 non-regression and C1 CI upper bound at most one"
                ),
                evidence_status=("EvidenceValid" if cold_miss_verified else "EvidenceUncovered"),
                regression_status="DiagnosticOnly" if pre_touch else "RegressionCompared",
                milestone_status="MilestoneNotPassed",
                model_status="ModelNotAdmitted",
            )
        except Exception as error:
            failures += 1
            result.update(status="failed", error=f"{type(error).__name__}: {error}",
                          failure_traceback=traceback.format_exc())
            # Report the originating failure before publishing its envelope;
            # archive validation must never hide the query/collection error.
            print(f"TPC-DS {query_id}: collection failed: {result['error']}", flush=True)
        report["queries"].append(result)
        report["passed"] = len(report["queries"]) - failures
        report["failed"] = failures
        report["faster_than_duckdb"] = sum(
            item.get("faster_than_duckdb", False) for item in report["queries"]
        )
        normal_payload, normal_receipts = normal_cell_evidence(result, normal_rows)
        output.publish_cell_json(
            query_case=query_id,
            arm_id="normal",
            payload=build_benchmark_cell_payload(
                campaign_id=output.run.campaign_id,
                run_id=output.run.run_id,
                query_case=query_id,
                arm_id="normal",
                workload_name="tpcds",
                query_payload=normal_payload,
                compile_receipts=normal_receipts,
                source_id=output.attempts[(query_id, "normal")].source_id,
                attempt_id=output.attempts[(query_id, "normal")].attempt_id,
            ),
        )
        output.publish_cell_json(
            query_case=query_id,
            arm_id="diagnostic",
            payload=build_benchmark_cell_payload(
                campaign_id=output.run.campaign_id,
                run_id=output.run.run_id,
                query_case=query_id,
                arm_id="diagnostic",
                workload_name="tpcds",
                query_payload={
                    "schema_version": EVIDENCE_SCHEMA_VERSION,
                    "query": query_id,
                    "diagnostic_cohort": result.get("diagnostic_cohort"),
                    "status": result.get("status"),
                },
                compile_receipts=[
                    uncovered_receipt(
                        "diagnostic Compile Evidence is not an execution receipt"
                    )
                    for _ in range(args.diagnostic_process_blocks)
                ],
                source_id=output.attempts[(query_id, "diagnostic")].source_id,
                attempt_id=output.attempts[(query_id, "diagnostic")].attempt_id,
            ),
        )
        output.publish_campaign_summary()
        if result["status"] != "passed":
            output_errors[(query_id, "normal")] = result.get("error", "query failed")
            output_errors[(query_id, "diagnostic")] = result.get("error", "query failed")
        if result["status"] == "passed":
            c1_confidence = result["cold_crossover"][
                "hierarchical_confidence_interval_95"
            ]
            print(
                f"TPC-DS {query_id}: {'pre-touched target (NOT C1)' if pre_touch else 'C1'} Paro {result['cold_statement']['paro']['median_ms']:.3f} ms, "
                f"DuckDB {result['cold_statement']['duckdb']['median_ms']:.3f} ms, "
                f"ratio {result['paro_over_duckdb']:.3f}, "
                f"95% CI [{c1_confidence[0]:.3f}, {c1_confidence[1]:.3f}]; "
                f"W ratio {result['warm_paro_over_duckdb']:.3f}",
                flush=True,
            )
        else:
            print(f"TPC-DS {query_id}: failed: {result['error']}", flush=True)

    output.control.write_json("corpus-impact.json", corpus_impact_summary(report["queries"]))
    output.publish_campaign_summary()
    output.finish(
        status="Incomplete" if failures else "Completed",
        errors=output_errors,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
