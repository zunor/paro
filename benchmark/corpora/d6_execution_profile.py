#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Collect diagnostic-only first-execution profiles for D6.

The primary C1 comparator deliberately does not run this collector.  Each
sample owns a fresh private Paro data copy and executes the original query
once through ``EXPLAIN ANALYZE``.  The complete wire payload is retained so
the text renderer and the structured profile parser can be audited together.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from benchmark.corpora.benchmark_evidence import (
    ImmutableDataSeed,
    build_benchmark_server,
    content_digest,
    isolated_paro_server,
    repository_identity,
    statement_fingerprint,
)
from benchmark.corpora.cold_planning import ProcessWatchdog
from benchmark.harness.executor import (
    _extract_explain_execution_time_ms,
    _flatten_explain_profile,
)


def _strip_sql(sql_text: str) -> str:
    query = sql_text.strip()
    if not query:
        raise ValueError("query SQL is empty")
    while query.endswith(";"):
        query = query[:-1].rstrip()
    if not query:
        raise ValueError("query SQL contains no statement")
    return query


def _fetch_all_text(connection: Any, query: str) -> list[str]:
    rows = connection.execute(query).fetchall()
    payload: list[str] = []
    for row in rows:
        if not row:
            continue
        value = row[0]
        if isinstance(value, str):
            payload.append(value)
        elif value is not None:
            payload.append(str(value))
    if not payload:
        raise ValueError("EXPLAIN ANALYZE returned no rows")
    return payload


def _pipeline_summary(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for profile in profiles:
        tree_path = str(profile.get("tree_path", "0"))
        pipeline = tree_path.split("/", 1)[0]
        grouped.setdefault(pipeline, []).append(profile)

    summary: list[dict[str, Any]] = []
    for pipeline, operators in sorted(grouped.items(), key=lambda item: int(item[0])):
        timed = [
            profile["total_time_ms"]
            for profile in operators
            if isinstance(profile.get("total_time_ms"), (int, float))
        ]
        rows = [
            profile["rows"]
            for profile in operators
            if isinstance(profile.get("rows"), int)
        ]
        summary.append(
            {
                "pipeline": int(pipeline),
                "operators": [
                    {
                        "tree_path": profile["tree_path"],
                        "operator": profile["operator"],
                        "runtime_node_id": profile.get("node_id"),
                        "logical_node_id": profile.get("logical_node_id"),
                        "rows": profile.get("rows"),
                        "loops": profile.get("loops"),
                        "startup_time_ms": profile.get("startup_time_ms"),
                        "total_time_ms": profile.get("total_time_ms"),
                        "reported_memory_bytes": profile.get("reported_memory_bytes"),
                        "scheduler_ready_time_us": profile.get("scheduler_ready_time_us"),
                        "scheduler_wait_time_us": profile.get("scheduler_wait_time_us"),
                        "runtime_filter_installed_count": profile.get(
                            "runtime_filter_installed_count"
                        ),
                        "aggregate_hash_max_radix_partition_skew_percent": profile.get(
                            "aggregate_hash_max_radix_partition_skew_percent"
                        ),
                    }
                    for profile in operators
                ],
                "max_operator_time_ms": max(timed, default=0.0),
                "max_rows": max(rows, default=0),
            }
        )
    return summary


def _sample(
    args: argparse.Namespace,
    binary: Path,
    query: str,
    block: int,
    seed: ImmutableDataSeed,
) -> dict[str, Any]:
    log = args.report.with_suffix(f".block{block}.parod.log")
    sample_id = f"d6.block{block}"
    result: dict[str, Any] = {"block": block, "status": "error", "log": str(log)}
    with isolated_paro_server(
        binary,
        seed,
        args.listen,
        log,
        max_memory=args.memory_limit,
        threads=args.threads,
        statement_trace=True,
        trace_sample_id=sample_id,
    ) as server:
        result["server"] = server.identity()
        if server.process is None:
            raise RuntimeError("managed Paro server has no process")
        with ProcessWatchdog(
            server.process,
            args.watchdog_seconds,
            args.rss_limit_mb * 1024 * 1024,
        ) as watchdog:
            started = time.perf_counter_ns()
            try:
                host, port = args.listen.rsplit(":", 1)
                with psycopg.connect(
                    host=host,
                    port=int(port),
                    dbname=args.database,
                    user=args.user,
                    autocommit=True,
                    connect_timeout=10,
                ) as connection:
                    connection.execute("SET optimizer_verify=true")
                    connection.execute(sql.SQL("SET threads={}").format(sql.Literal(args.threads)))
                    connection.execute(
                        sql.SQL("SET memory_limit={}").format(sql.Literal(args.memory_limit))
                    )
                    connection.execute(
                        sql.SQL("SET statement_timeout={}").format(
                            sql.Literal(args.watchdog_seconds * 1000)
                        )
                    )
                    payload = _fetch_all_text(connection, f"EXPLAIN ANALYZE {query}")
                raw = "\n".join(payload)
                result.update(
                    {
                        "status": "ok",
                        "client_elapsed_ms": (time.perf_counter_ns() - started) / 1_000_000,
                        "rows": payload,
                        "row_count": len(payload),
                        "execution_time_ms": _extract_explain_execution_time_ms(raw),
                        "operators": _flatten_explain_profile(raw),
                    }
                )
            except Exception as error:  # pragma: no cover - exercised by live collector
                result["error"] = f"{type(error).__name__}: {error}"
        result["peak_rss_bytes"] = watchdog.peak_rss
        if watchdog.failure:
            result.update(status="error", error=watchdog.failure)
    result["query_fingerprint"] = statement_fingerprint(f"EXPLAIN ANALYZE {query}")
    result["pipeline_summary"] = _pipeline_summary(result.get("operators", []))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-data-dir", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:6432")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--process-blocks", type=int, default=2)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--watchdog-seconds", type=int, default=300)
    parser.add_argument("--rss-limit-mb", type=int, default=2048)
    parser.add_argument("--build-jobs", type=int, default=4)
    args = parser.parse_args()
    if min(args.process_blocks, args.threads, args.watchdog_seconds, args.rss_limit_mb) < 1:
        parser.error("blocks, threads, deadline and RSS must be positive")

    root = Path(__file__).resolve().parents[2]
    query_path = args.query.resolve()
    query = _strip_sql(query_path.read_text(encoding="utf-8"))
    binary, build = build_benchmark_server(root, args.build_jobs)
    seed = ImmutableDataSeed.capture(args.server_data_dir)
    report: dict[str, Any] = {
        "schema_version": 1,
        "mode": "d6_execution_profile",
        "query_path": str(query_path),
        "query_sha256": content_digest(query_path),
        "query_fingerprint": statement_fingerprint(f"EXPLAIN ANALYZE {query}"),
        "seed": {"path": str(seed.path), "sha256": seed.sha256},
        "binary": build,
        "source_identity": repository_identity(root),
        "configuration": {
            "process_blocks": args.process_blocks,
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "watchdog_seconds": args.watchdog_seconds,
            "rss_limit_mb": args.rss_limit_mb,
            "cohort": "diagnostic",
            "statement_trace": True,
            "normal_c1_included": False,
            "page_cache_policy": "private_copy_per_process; OS cache not flushed",
        },
        "machine": {
            "system": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "host": platform.node(),
            "logical_cpus": os.cpu_count(),
        },
        "samples": [],
    }
    for block in range(args.process_blocks):
        measurement = _sample(args, binary, query, block, seed)
        report["samples"].append(measurement)
        print(
            f"D6 block {block}: {measurement.get('execution_time_ms', '?')} ms, "
            f"{measurement['status']}",
            flush=True,
        )
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    if any(sample.get("status") != "ok" for sample in report["samples"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
