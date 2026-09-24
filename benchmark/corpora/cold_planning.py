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
        CompileEvidenceCollector,
        plan_structure_id,
        repository_identity,
        statement_fingerprint,
        tree_digest,
    )
    from benchmark.harness.run_output import CampaignOutput
    from benchmark.harness.receipt_contract import (
        EVIDENCE_SCHEMA_VERSION,
        build_benchmark_cell_payload,
        uncovered_receipt,
        validate_compile_document,
    )
except ModuleNotFoundError:  # pragma: no cover - script-only import path
    from benchmark_evidence import (
        ImmutableDataSeed,
        isolated_paro_server,
        build_benchmark_server,
        content_digest,
        CompileEvidenceCollector,
        plan_structure_id,
        repository_identity,
        statement_fingerprint,
        tree_digest,
    )
    from harness.run_output import CampaignOutput
    from harness.receipt_contract import (
        EVIDENCE_SCHEMA_VERSION,
        build_benchmark_cell_payload,
        uncovered_receipt,
        validate_compile_document,
    )

_REQUIRED_COMPILE_COUNTERS = {
    "search_complete",
    "memo_group_count",
    "memo_logical_expression_count",
    "memo_physical_expression_count",
    "settlement_local_hit_count",
    "settlement_local_miss_count",
    "search_rule_failure_count",
    "search_deadline_reached",
}


def _observed(document: dict[str, Any], field: str) -> Any:
    value = document.get(field)
    if not isinstance(value, dict) or set(value) != {"Observed"}:
        raise ValueError(f"compile document field {field!r} is not observed")
    return value["Observed"]


def _typed_compile_measurements(document: dict[str, Any]) -> dict[str, Any]:
    """Read all cold-planning metrics from one Rust-owned v3 document.

    ``paro_optimizers()`` is intentionally not consulted here.  Its receipt
    rows describe ordinary execution and may have a different schema; they
    are never a source for compile timing or optimizer counters.
    """
    if validate_compile_document(document) != "Summary":
        raise ValueError("cold planning requires a v3 compile summary")
    omitted_counters = document.get("omitted_search_counters")
    if omitted_counters != 0:
        raise ValueError("cold planning requires a complete search counter snapshot")
    optimizer_ns = _observed(document, "optimizer_ns")
    if isinstance(optimizer_ns, bool) or not isinstance(optimizer_ns, int) or optimizer_ns < 0:
        raise ValueError("compile document has invalid optimizer duration")
    counters = {
        counter["name"]: counter["value"]
        for counter in document.get("search_counters", [])
    }
    missing = _REQUIRED_COMPILE_COUNTERS - counters.keys()
    if missing:
        raise ValueError(
            "compile document lacks required search counters: "
            + ", ".join(sorted(missing))
        )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in counters.values()
    ):
        raise ValueError("compile document has a non-integer search counter")
    search_complete = _observed(document, "search_complete")
    if not isinstance(search_complete, bool):
        raise ValueError("compile document has invalid search completion state")
    counters["search_complete"] = int(search_complete)
    stop = _observed(document, "search_stop")
    if not isinstance(stop, str) or not stop:
        raise ValueError("compile document has invalid search stop reason")
    return {
        "optimizer_ms": optimizer_ns / 1_000_000,
        "counters": counters,
        "omitted_search_counters": omitted_counters,
        "rules": document.get("rules", []),
        "search_stop": stop,
        "compile_metrics_source": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document",
        "diagnostics": [],
        "diagnostics_source": "not_collected; paro_optimizers is execution-only auxiliary",
    }


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
                    connection.execute(sql.SQL("SET optimizer_verify={}").format(sql.Literal(args.optimizer_verify == "on")))
                    connection.execute(sql.SQL("SET optimizer_search_policy={}").format(sql.Literal(args.optimizer_search_policy)))
                    connection.execute(sql.SQL("SET threads={}").format(sql.Literal(args.threads)))
                    connection.execute(sql.SQL("SET memory_limit={}").format(sql.Literal(args.memory_limit)))
                    connection.execute(sql.SQL("SET statement_timeout={}").format(
                        sql.Literal(args.watchdog_seconds * 1000)))
                    started = time.perf_counter_ns()
                    # The compile document is the sole diagnostic producer.
                    # Do not run a second structural EXPLAIN: that would
                    # compile the target twice and would break the receipt
                    # association used by the collector.
                    raw_document, compile_document = CompileEvidenceCollector(
                        connection
                    ).capture(query, detail=True)
                    result["explain_wall_ms"] = (time.perf_counter_ns() - started) / 1_000_000
                    result["compile_document"] = compile_document
                    result["compile_document_raw"] = raw_document
                    result["plan_format"] = "compile-json"
                    result["plan_structure_id"] = plan_structure_id(compile_document)
                    result.update(_typed_compile_measurements(compile_document))
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


def _detach_compile_capture(
    owned: CampaignOutput,
    *,
    query_case: str,
    measurement: dict[str, Any],
) -> dict[str, Any]:
    """Persist the raw typed document once and leave a bounded reference."""
    raw = measurement.get("compile_document_raw")
    document = measurement.get("compile_document")
    if not isinstance(raw, str) or not isinstance(document, dict):
        return measurement
    capture_name = f"block-{measurement['block']:04d}.json"
    capture_path = owned.publish_capture_text(
        query_case=query_case,
        arm_id="diagnostic",
        name=capture_name,
        text=raw,
    )
    relative = capture_path.relative_to(owned.run.root).as_posix()
    bounded = dict(measurement)
    bounded.pop("compile_document_raw", None)
    bounded["compile_document"] = {
        "status": "Captured",
        "path": relative,
        "sha256": content_digest(capture_path),
        "schema_version": document.get("schema_version"),
    }
    return bounded


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
    parser.add_argument("--optimizer-search-policy", choices=("regional", "quality", "budgeted"), default="quality")
    parser.add_argument("--optimizer-verify", choices=("on", "off"), default="on")
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
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "compile_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
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
        "optimizer_search_policy": args.optimizer_search_policy,
        "optimizer_verify": args.optimizer_verify == "on",
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
        "compile_metrics_source": "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) typed document",
        "execution_receipt_source": "not_collected; EXPLAIN target is not executed",
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
                "summary_captures": args.process_blocks,
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
            measurement = _detach_compile_capture(
                owned, query_case=path.stem, measurement=measurement
            )
            observation["samples"].append(measurement)
            print(f"{path.stem} block {block}: {measurement.get('explain_wall_ms', '?')} ms, "
                  f"{measurement['status']}", flush=True)
            owned.publish_cell_json(
                query_case=path.stem,
                arm_id="diagnostic",
                payload=build_benchmark_cell_payload(
                    campaign_id=owned.run.campaign_id,
                    run_id=owned.run.run_id,
                    query_case=path.stem,
                    arm_id="diagnostic",
                    workload_name="cold_planning",
                    query_payload={
                        "schema_version": report["schema_version"],
                        "configuration": report["configuration"],
                        "evidence": report["evidence"],
                        "query": observation,
                    },
                    compile_receipts=[
                        uncovered_receipt(
                            "diagnostic Compile Evidence is not an execution receipt"
                        )
                        for _ in observation["samples"]
                    ],
                    source_id=owned.attempts[(path.stem, "diagnostic")].source_id,
                    attempt_id=owned.attempts[(path.stem, "diagnostic")].attempt_id,
                ),
            )
            owned.publish_campaign_summary()
        if any(sample_item.get("status") != "ok" for sample_item in observation["samples"]):
            failures[(path.stem, "diagnostic")] = "one or more cold-planning samples failed"
    if (repository_identity(root) != build["source"] or content_digest(binary) != build["binary_sha256"]
            or tree_digest(args.server_data_dir) != report["evidence"]["dataset_sha256"]
            or any(content_digest(path) != query["sql_sha256"] for path, query in zip(args.query, report["queries"], strict=True))):
        report["invalidated"] = "source, SQL, dataset or binary changed during measurements"
        owned.publish_campaign_summary()
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
