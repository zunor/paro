#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Measure the first EXPLAIN in a fresh, owned server, with an external watchdog.

The SQL is never rewritten. Startup/connection/SET costs are outside the timer;
EXPLAIN wall time includes transport, parse, bind, optimize and plan rendering.
Optimizer component time is reported separately. Allocation metrics are a
separate build/configuration, never compared to an uninstrumented baseline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

try:
    # Package imports are used by unit tests and the D6 collector.  Keep the
    # script form below working for the documented ``python benchmark/corpora``
    # invocation as well.
    from benchmark.corpora.benchmark_evidence import (
        ImmutableDataSeed,
        isolated_paro_server,
        build_benchmark_server,
        content_digest,
        fetch_compile_document,
        repository_identity,
        statement_fingerprint,
        tree_digest,
    )
    from benchmark.harness.run_output import CampaignOutput
except ModuleNotFoundError:  # pragma: no cover - script-only import path
    from benchmark_evidence import (
        ImmutableDataSeed,
        isolated_paro_server,
        build_benchmark_server,
        content_digest,
        fetch_compile_document,
        repository_identity,
        statement_fingerprint,
        tree_digest,
    )
    from harness.run_output import CampaignOutput

COMPONENTS = {"semantic_normalization", "query_ir_construction", "direct_physical_search",
              "memo_exploration", "physical_extraction", "winner_verification"}


def diagnostic_rows(columns: list[str], rows: list[tuple]) -> list[dict[str, Any]]:
    # pgwire may qualify unaliased system-function outputs. Accept the declared
    # relation prefix, but never silently zip missing/duplicate metric fields.
    columns = [name.removeprefix("paro_optimizers.") for name in columns]
    expected = {"name", "kind", "last_elapsed_us", "metric_value", "metric_unit", "invocation_count"}
    if len(columns) != len(expected) or set(columns) != expected:
        raise ValueError("optimizer diagnostic schema differs from the measurement contract")
    return [dict(zip(columns, row, strict=True)) for row in rows]


class ProcessWatchdog:
    """Bound a single owned process even when cooperative query checks stall."""

    def __init__(self, process: subprocess.Popen, seconds: float, rss_limit: int) -> None:
        self.process, self.seconds, self.rss_limit = process, seconds, rss_limit
        self.peak_rss = 0
        self.failure: str | None = None
        self.done = threading.Event()
        self.worker = threading.Thread(target=self._watch, daemon=True)

    def _watch(self) -> None:
        started = time.monotonic()
        while not self.done.is_set() and self.process.poll() is None:
            try:
                output = subprocess.check_output(["ps", "-o", "rss=", "-p", str(self.process.pid)],
                                                 timeout=2, text=True)
                self.peak_rss = max(self.peak_rss, int(output.strip()) * 1024)
            except (ValueError, subprocess.SubprocessError):
                self.failure = "RSS observation failed"
            if self.peak_rss > self.rss_limit:
                self.failure = "external RSS limit exceeded"
            if time.monotonic() - started > self.seconds:
                self.failure = "external wall deadline exceeded"
            if self.failure:
                # Popen refers only to the child created for this sample. Never
                # search/kill by port, binary name or an unowned numeric PID.
                self.process.kill()
                return
            self.done.wait(0.02)

    def __enter__(self) -> "ProcessWatchdog":
        self.worker.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.done.set()
        self.worker.join(timeout=3)


def sample(args: argparse.Namespace, binary: Path, query: str, name: str, block: int,
           seed: ImmutableDataSeed) -> dict[str, Any]:
    result: dict[str, Any] = {"block": block, "status": "error"}
    with isolated_paro_server(binary, seed, args.listen, None,
                           max_memory=args.memory_limit, threads=args.threads,
                           statement_trace=False) as server:
        result["server"] = server.identity()
        assert server.process is not None
        with ProcessWatchdog(server.process, args.watchdog_seconds,
                             args.rss_limit_mb * 1024 * 1024) as watchdog:
            try:
                host, port = args.listen.rsplit(":", 1)
                with psycopg.connect(host=host, port=int(port), dbname=args.database,
                                     user=args.user, autocommit=True, connect_timeout=10) as connection:
                    connection.execute("SET optimizer_verify=true")
                    connection.execute(sql.SQL("SET threads={}").format(sql.Literal(args.threads)))
                    connection.execute(sql.SQL("SET memory_limit={}").format(sql.Literal(args.memory_limit)))
                    connection.execute(sql.SQL("SET statement_timeout={}").format(
                        sql.Literal(args.watchdog_seconds * 1000)))
                    started = time.perf_counter_ns()
                    # The compile document is the sole diagnostic producer.
                    # Do not run a second structural EXPLAIN: that would
                    # compile the target twice and would break the receipt
                    # association used by the collector.
                    raw_document, compile_document = fetch_compile_document(
                        connection, query, detail=True
                    )
                    result["explain_wall_ms"] = (time.perf_counter_ns() - started) / 1_000_000
                    result["compile_document"] = compile_document
                    result["compile_document_raw"] = raw_document
                    result["plan_format"] = "compile-json"
                    result["plan_sha256"] = hashlib.sha256(raw_document.encode()).hexdigest()
                    cursor = connection.execute("SELECT * FROM paro_optimizers()")
                    columns = [column.name for column in cursor.description or ()]
                    diagnostics = diagnostic_rows(columns, cursor.fetchall())
                    result["diagnostics"] = diagnostics
                    seen = {row["name"] for row in diagnostics if row["name"] in COMPONENTS}
                    if seen != COMPONENTS:
                        raise RuntimeError("missing optimizer component diagnostics")
                    result["optimizer_ms"] = sum(row["last_elapsed_us"] for row in diagnostics
                                                 if row["name"] in COMPONENTS) / 1000
                    result["counters"] = {row["name"]: row["metric_value"] for row in diagnostics
                                          if row["kind"] == "search_counter" and row["metric_unit"] == "count"}
                    result["status"] = "ok"
            except Exception as error:
                result["error"] = f"{type(error).__name__}: {error}"
        result["peak_rss_bytes"] = watchdog.peak_rss
        if watchdog.failure:
            result.update(status="error", error=watchdog.failure)
    result["compile_query_fingerprint"] = statement_fingerprint(
        f"EXPLAIN (COMPILE, DETAIL, FORMAT JSON) {query}"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-data-dir", type=Path, required=True)
    parser.add_argument("--query", type=Path, action="append", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:6432")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--process-blocks", type=int, default=5)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--watchdog-seconds", type=int, default=30)
    parser.add_argument("--rss-limit-mb", type=int, default=2048)
    parser.add_argument("--alloc-metrics", action="store_true")
    parser.add_argument("--build-jobs", type=int, default=4)
    args = parser.parse_args()
    if min(args.process_blocks, args.watchdog_seconds, args.rss_limit_mb, args.threads) < 1:
        parser.error("blocks, time, RSS and threads must be positive")
    if len({path.stem for path in args.query}) != len(args.query):
        parser.error("query filenames must have unique stems")
    root = Path(__file__).resolve().parents[2]
    binary, build = build_benchmark_server(root, args.build_jobs,
                                           features=("alloc-metrics",) if args.alloc_metrics else ())
    seed = ImmutableDataSeed.capture(args.server_data_dir)
    harness_files = (Path(__file__).resolve(), Path(__file__).with_name("benchmark_evidence.py"),
                     root / "benchmark/harness/cold_planning_gate.py")
    report: dict[str, Any] = {
        "schema_version": 5,
        "configuration": {key: getattr(args, key) for key in ("process_blocks", "threads", "memory_limit",
                             "watchdog_seconds", "rss_limit_mb", "alloc_metrics")},
        "evidence": {"build": build, "dataset_sha256": seed.sha256,
                     "dataset_path": str(seed.path),
                     "machine": {"system": platform.platform(), "machine": platform.machine(),
                                 "processor": platform.processor(), "host": platform.node(),
                                 "logical_cpus": os.cpu_count()},
                     "harness_sha256": hashlib.sha256("".join(content_digest(p) for p in harness_files).encode()).hexdigest(),
                     "harness": [{"path": str(p), "sha256": content_digest(p)} for p in harness_files]},
        "queries": [{"name": path.stem, "path": str(path.resolve()), "sql_sha256": content_digest(path),
                     "samples": []} for path in args.query],
    }
    report["configuration"].update({
        "planning_dop": 1,
        "execution_dop": args.threads,
        "resource_envelope": {
            "service_cpu_limit": args.threads,
            "memory_limit": args.memory_limit,
            "concurrent_target_statements": 1,
            "page_cache_policy": "private_copy_per_process; OS cache not flushed",
        },
        "cohort": "diagnostic",
        "trace_mode": "off",
        "compile_document": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON)",
        "latency_tracks": {
            "C0": {"status": "uncovered", "auxiliary": "startup_to_ready_ms"},
            "C1": {"status": "uncovered", "reason": "EXPLAIN is not the target C1"},
            "C2": {"status": "uncovered", "reason": "partial diagnostic interval only"},
            "W": "not measured by cold-planning collector",
        },
        "model_gates": {
            "version": 1,
            "G-Stats": {
                "status": "registered_not_admitted",
                "thresholds": {"q_error_p95_max": 2.0, "q_error_max": 4.0},
            },
            "G-Cost": {
                "status": "registered_not_admitted",
                "thresholds": {"phase_time_prediction_p95_ratio_max": 1.25},
            },
        },
    })
    report["configuration"]["runtime_environment"] = {
        "RUST_LOG": os.environ.get("RUST_LOG"),
        "PARO_STATEMENT_TRACE": "0",
    }
    owned = CampaignOutput.create(
        args.report,
        source_id="cold_planning",
        cells=[
            {
                "query_case": path.stem,
                "arm_id": "diagnostic",
                "query_cases": 1,
                "sample_rows": args.process_blocks,
                "product_receipts": args.process_blocks,
                "summary_captures": 1,
            }
            for path in args.query
        ],
    )
    failures: dict[tuple[str, str], str] = {}
    for path, observation in zip(args.query, report["queries"], strict=True):
        query = path.read_text().strip()
        while query.endswith(";"):
            query = query[:-1].rstrip()
        for block in range(args.process_blocks):
            try:
                # Startup mutates owner/checkpoint metadata even for EXPLAIN.
                # Every sample starts from the same immutable seed, not the
                # preceding process's database. Copying is outside the timer.
                measurement = sample(args, binary, query, path.stem, block, seed)
            except Exception as error:
                measurement = {"block": block, "status": "error", "error": f"{type(error).__name__}: {error}"}
            observation["samples"].append(measurement)
            print(f"{path.stem} block {block}: {measurement.get('explain_wall_ms', '?')} ms, "
                  f"{measurement['status']}", flush=True)
            owned.publish_cell_json(
                query_case=path.stem,
                arm_id="diagnostic",
                payload={
                    "schema_version": report["schema_version"],
                    "configuration": report["configuration"],
                    "evidence": report["evidence"],
                    "query": observation,
                },
            )
            owned.publish_campaign_json(report)
        if any(sample_item.get("status") != "ok" for sample_item in observation["samples"]):
            failures[(path.stem, "diagnostic")] = "one or more cold-planning samples failed"
    if (repository_identity(root) != build["source"] or content_digest(binary) != build["binary_sha256"]
            or tree_digest(args.server_data_dir) != report["evidence"]["dataset_sha256"]
            or any(content_digest(path) != query["sql_sha256"] for path, query in zip(args.query, report["queries"], strict=True))):
        report["invalidated"] = "source, SQL, dataset or binary changed during measurements"
        owned.publish_campaign_json(report)
        failures.update(
            {
                (path.stem, "diagnostic"): report["invalidated"]
                for path in args.query
            }
        )
    owned.control.write_text(
        "summary.md",
        "Cold-planning Compile Evidence\n"
        f"queries={len(report['queries'])}\n"
        f"status={'Incomplete' if failures else 'Completed'}\n",
        overwrite=True,
    )
    owned.finish(status="Incomplete" if failures else "Completed", errors=failures)
    if failures:
        return 1
    return 0 if all(s["status"] == "ok" for q in report["queries"] for s in q["samples"]) else 1


if __name__ == "__main__":
    sys.exit(main())
