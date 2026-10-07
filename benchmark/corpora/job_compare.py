#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Exploratory warm JOB baseline using maintained process/timer/result helpers.

Each block owns fresh Paro and DuckDB processes; the query sequence shares each
process. First occurrences are warmups, not per-query cold-start measurements.
No release/parity certification or expected-result regeneration is performed.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import random
import statistics
import traceback

import _duckdb
import duckdb
import psycopg

from benchmark_evidence import (CompileEvidenceCollector, ImmutableDataSeed,
    build_benchmark_server, content_digest, isolated_paro_server, repository_identity)
from job_setup import corpus_inputs, input_identity, paro_ddl_inputs, read_manifest, write_json
from tpcds import read_answer
from tpcds_compare import (DuckDBProcess, configure_paro, extension_digest,
    schema_report, timed_run_paro, timing_summary, CONSTRAINTS_SQL)
from tpcds_result_contract import (RESULT_CONTRACT_VERSION, assert_compatible_schema,
    assert_same_multiset, canonicalize_rows, multiset_digest)


@contextmanager
def measurement_lock(path: Path):
    # Refuse an occupied shared resource; never delete another task's lock.
    path.mkdir()
    owner = path / "owner"
    try:
        owner.write_text(f"JOB comparison pid={os.getpid()}\n", encoding="utf-8")
        yield
    finally:
        owner.unlink(missing_ok=True)
        path.rmdir()


def query_paths(checkout: Path, selection: str) -> list[Path]:
    root = checkout / "benchmark/imdb_plan_cost/queries"
    available = {path.stem: path for path in root.glob("*.sql")}
    if not available:
        raise ValueError("JOB SQL corpus is empty")
    if selection == "all":
        return [available[key] for key in sorted(available)]
    keys = selection.split(",")
    if len(set(keys)) != len(keys) or any(key not in available for key in keys):
        raise ValueError("queries must be distinct exact JOB stems, such as 01a,17b")
    return [available[key] for key in keys]


def answer_rows(path: Path, schema) -> list[tuple]:
    names, rows = read_answer(path)
    if len(names) != len(schema):
        raise AssertionError(f"JOB answer arity differs from oracle: {path}")
    def decode(value, column):
        if value == "NULL":
            return None
        if column.logical_type.startswith(("int", "uint")):
            return int(value)
        if column.logical_type == "string":
            return value
        raise ValueError(f"JOB answer needs an explicit decoder for {column.logical_type}")
    return canonicalize_rows([tuple(decode(v, c) for v, c in zip(row, schema)) for row in rows], schema)


def validate_result(query: str, rows, schema, oracle_schema, expected, *, engine="paro") -> str:
    if engine == "paro":
        assert_compatible_schema(schema, oracle_schema, query=query)
    elif engine == "duckdb":
        assert_compatible_schema(schema, oracle_schema)
    else:
        raise ValueError(f"unknown JOB result engine: {engine}")
    canonical = canonicalize_rows(rows, schema)
    assert_same_multiset(canonical, expected)
    return multiset_digest(canonical)


def validate_import_receipt(receipt: dict, manifest: dict, manifest_digest: str,
                            seed_digest: str, ddl: dict) -> None:
    """Bind a completed importer record to this immutable data and exact DDL."""
    if (receipt.get("status") != "completed"
        or receipt.get("dataset_manifest_sha256") != manifest_digest
        or receipt.get("seed", {}).get("sha256") != seed_digest):
        raise ValueError("JOB import receipt completion/manifest/seed identity mismatch")
    tables = manifest["export"]["tables"]
    expected = receipt.get("expected_input_tables", {})
    verified = receipt.get("verified_row_counts", {})
    if set(expected) != set(tables) or set(verified) != set(tables):
        raise ValueError("JOB import receipt table coverage mismatch")
    for table, record in tables.items():
        if (type(expected[table].get("rows")) is not int
            or type(expected[table].get("csv_bytes")) is not int
            or any(expected[table].get(field) != record[field]
                for field in ("rows", "csv_sha256", "csv_bytes"))
            or type(verified[table]) is not int or verified[table] != record["rows"]):
            raise ValueError(f"JOB import receipt CSV/count mismatch: {table}")
    recorded_ddl = receipt.get("paro_ddl", {})
    if any(recorded_ddl.get(field) != value for field, value in ddl.items()):
        raise ValueError("JOB import receipt normalized DDL identity mismatch")


def run(args) -> int:
    repo = Path(__file__).resolve().parents[2]
    checkout = args.duckdb_checkout.resolve()
    root = args.data_root.resolve()
    corpus = input_identity(checkout)
    manifest = read_manifest(root)
    if manifest["source"] != corpus or manifest.get("export", {}).get("metadata_track") != "none":
        raise ValueError("JOB baseline requires this corpus's exported metadata-none dataset")
    database = root / "job.duckdb"
    database_digest = content_digest(database)
    if database_digest != manifest["export"]["database_sha256"]:
        raise ValueError("JOB DuckDB database differs from its export manifest")
    selected = query_paths(checkout, args.queries)
    declared_tables = {table for table, _ in corpus_inputs(checkout)[0]}
    if set(manifest["export"]["tables"]) != declared_tables:
        raise ValueError("JOB export manifest does not cover all 21 declared tables")
    if args.report.exists():
        raise ValueError("report already exists; select a fresh owned run path")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    artifact_root = args.report.with_name(args.report.stem + "-run")
    artifact_root.mkdir()
    binary, build = build_benchmark_server(repo, args.build_jobs)
    seed = ImmutableDataSeed.capture(args.server_data_dir)
    import_record = {"coverage": "Uncovered: no completed import receipt supplied"}
    if args.import_receipt is not None:
        receipt = json.loads(args.import_receipt.read_text(encoding="utf-8"))
        validate_import_receipt(receipt, manifest, content_digest(root / "dataset.json"),
                                seed.sha256, paro_ddl_inputs(checkout)[1])
        import_record = {"coverage": "Bound: completed import receipt",
                         "path": str(args.import_receipt.resolve()),
                         "sha256": content_digest(args.import_receipt), "record": receipt}
    source = repository_identity(repo)
    if source != build["source"]:
        raise RuntimeError("JOB source changed after its binary was built")
    report = {
        "schema_version": 1, "corpus": "JOB", "purpose": "exploratory warm baseline",
        "source": source, "build_attestation": build, "corpus_source": corpus,
        "dataset": {"manifest_sha256": content_digest(root / "dataset.json"),
                    "duckdb_sha256": database_digest, "paro_seed_sha256": seed.sha256,
                    "metadata_track": "none",
                    "import_lineage": import_record,
                    "statistics_policy": {"duckdb": "ANALYZE during export",
                                          "paro": "storage-maintained ingestion statistics; SQL ANALYZE unsupported"}},
        "duckdb": {"version": duckdb.__version__, "native_extension": extension_digest(_duckdb)},
        "configuration": {"threads": args.threads, "memory_limit": args.memory_limit,
                          "statement_timeout_seconds": args.statement_timeout_seconds,
                          "optimizer_verify": True, "process_blocks": args.process_blocks,
                          "warmups": args.warmups, "rounds": args.rounds, "seed": args.random_seed,
                          "query_order": [p.stem for p in selected],
                          "result_contract": RESULT_CONTRACT_VERSION,
                          "timer": "maintained tpcds_compare execute/fetch/native_metadata helpers",
                          "cohort": "warm; first occurrence excluded; shared query sequence per process",
                          "normal_receipts": "Uncovered: this exploratory collector does not collect them",
                          "os_cache": "not flushed", "trace": False},
        "blocks": [], "summary": {}, "status": "running",
    }
    write_json(args.report, report)
    rng = random.Random(args.random_seed)
    failed = False
    for block in range(args.process_blocks):
        record = {"block": block, "queries": []}
        report["blocks"].append(record)
        with isolated_paro_server(binary, seed, args.listen, artifact_root / f"job-server-{block}.log",
             max_memory=args.memory_limit, threads=args.threads,
             optimizer_environment={"PARO_COLD_WORK_EVIDENCE": None, "PARO_ALLOC_AUDIT": None,
                                    "PARO_DIAGNOSTIC_STREAM_SEQUENTIAL": None}) as server:
            record["paro_process"] = server.identity()
            with DuckDBProcess(database, args.threads, args.memory_limit) as worker:
                record["duckdb_process"] = worker.identity
                def duck_execute(query):
                    return worker.execute(query, timeout_seconds=args.statement_timeout_seconds)
                host, port = args.listen.rsplit(":", 1)
                with psycopg.connect(host=host, port=int(port), user=args.user,
                     dbname=args.database, autocommit=True) as connection:
                    configure_paro(connection, args)
                    settings = connection.execute("SELECT current_setting('optimizer_verify'), current_setting('threads'), current_setting('memory_limit')").fetchone()
                    record["effective_paro_settings"] = list(settings)
                    if str(settings[0]).lower() not in {"true", "on", "1"}:
                        raise AssertionError("JOB requires effective optimizer_verify=true")
                    tables = manifest["export"]["tables"]
                    record["preflight_row_counts"] = {}
                    for table, exported in tables.items():
                        count_sql = f'SELECT count(*) FROM "{table}"'
                        paro_count = connection.execute(count_sql).fetchone()[0]
                        duck_count = duck_execute(count_sql)[0][0][0]
                        if paro_count != exported["rows"] or duck_count != exported["rows"]:
                            raise AssertionError(f"JOB preflight row count mismatch: {table}")
                        record["preflight_row_counts"][table] = int(paro_count)
                    paro_keys = [list(row) for row in connection.execute(CONSTRAINTS_SQL).fetchall() if row[0] in tables]
                    duck_keys = [list(row) for row in duck_execute(CONSTRAINTS_SQL)[0] if row[0] in tables]
                    record["key_inventories"] = {"paro": paro_keys, "duckdb": duck_keys}
                    if paro_keys or duck_keys:
                        raise AssertionError("JOB metadata-none baseline contains unexpected keys")
                    for path in selected:
                        cell = {"query": path.stem, "sql_sha256": content_digest(path), "status": "running",
                                "samples": [], "round_orders": []}
                        record["queries"].append(cell)
                        query = path.read_text(encoding="utf-8")
                        try:
                            oracle, oracle_schema, _ = duck_execute(query)
                            expected = canonicalize_rows(oracle, oracle_schema)
                            answer = checkout / "benchmark/imdb/answers" / f"{path.stem}.csv"
                            assert_same_multiset(expected, answer_rows(answer, oracle_schema))
                            cell["answer_sha256"] = content_digest(answer)
                            cell["oracle_schema"] = schema_report(oracle_schema)
                            cell["result_sha256"] = multiset_digest(expected)
                            for _ in range(args.warmups):
                                rows, schema, _ = timed_run_paro(connection, query, True)
                                validate_result(query, rows, schema, oracle_schema, expected)
                                rows, schema, _ = duck_execute(query)
                                validate_result(query, rows, schema, oracle_schema, expected, engine="duckdb")
                            for round_idx in range(args.rounds):
                                order = ["paro", "duckdb", "duckdb", "paro"]
                                if rng.getrandbits(1):
                                    order = ["duckdb" if engine == "paro" else "paro" for engine in order]
                                cell["round_orders"].append(order)
                                for position, engine in enumerate(order):
                                    rows, schema, elapsed = (timed_run_paro(connection, query, True)
                                        if engine == "paro" else duck_execute(query))
                                    sample = {"round": round_idx, "position": position, "engine": engine,
                                              "elapsed_ms": elapsed, "validation": "pending"}
                                    cell["samples"].append(sample)
                                    sample["result_sha256"] = validate_result(query, rows, schema, oracle_schema, expected, engine=engine)
                                    sample["validation"] = "passed"
                            if args.collect_plans:
                                raw, document = CompileEvidenceCollector(connection).capture(query, detail=True)
                                plan = artifact_root / f"job-{path.stem}-block-{block}-compile.json"
                                plan.write_text(raw, encoding="utf-8")
                                cell["diagnostic_compile"] = {"path": str(plan), "sha256": content_digest(plan), "schema_version": document["schema_version"]}
                            cell["status"] = "passed"
                        except Exception as error:
                            failed = True
                            cell["status"] = "failed"
                            cell["error"] = f"{type(error).__name__}: {error}"
                            cell["error_traceback"] = traceback.format_exc(limit=8)
                        write_json(args.report, report)
                        print(f"block={block} {path.stem}: {cell['status']}", flush=True)
        seed.verify_unchanged()
    for path in selected:
        cells = [cell for block in report["blocks"] for cell in block["queries"] if cell["query"] == path.stem]
        summary = {"status": "passed" if all(c["status"] == "passed" for c in cells) else "failed"}
        if summary["status"] == "passed":
            for engine in ("paro", "duckdb"):
                # Keep process blocks visible: pooled statistics are exploratory only.
                summary[engine] = {"blocks": [timing_summary([s["elapsed_ms"] for s in c["samples"] if s["engine"] == engine]) for c in cells]}
                summary[engine]["mean_block_median_ms"] = statistics.mean(b["median_ms"] for b in summary[engine]["blocks"])
            summary["paro_over_duckdb"] = summary["paro"]["mean_block_median_ms"] / summary["duckdb"]["mean_block_median_ms"]
        report["summary"][path.stem] = summary
    if (repository_identity(repo) != source or content_digest(binary) != build["binary_sha256"]
        or input_identity(checkout) != corpus or content_digest(database) != database_digest
        or content_digest(root / "dataset.json") != report["dataset"]["manifest_sha256"]):
        raise RuntimeError("JOB source/binary/corpus/data changed during comparison")
    if args.import_receipt is not None and content_digest(args.import_receipt) != import_record["sha256"]:
        raise RuntimeError("JOB import receipt changed during comparison")
    report["status"] = "failed" if failed else "passed"
    write_json(args.report, report)
    return int(failed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-checkout", required=True, type=Path)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--server-data-dir", required=True, type=Path)
    parser.add_argument("--import-receipt", type=Path,
                        help="completed post-shutdown importer record binding CSV/DDL to the immutable seed")
    parser.add_argument("--measurement-lock", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--listen", required=True)
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--queries", default="all")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--process-blocks", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--build-jobs", type=int, default=4)
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument("--collect-plans", action="store_true")
    args = parser.parse_args()
    args.report = args.report.resolve()
    if args.report.is_relative_to(args.server_data_dir.resolve()):
        parser.error("report must be outside the immutable server seed")
    if args.report.exists():
        parser.error("report already exists; use a fresh owned run path")
    args.optimizer_verify = "on"
    if min(args.threads, args.process_blocks, args.warmups, args.rounds, args.build_jobs, args.statement_timeout_seconds) < 1:
        parser.error("threads, blocks, warmups, rounds, jobs and timeout must be positive")
    with measurement_lock(args.measurement_lock):
        try:
            return run(args)
        except BaseException as error:
            if args.report.is_file():
                report = json.loads(args.report.read_text(encoding="utf-8"))
                report["status"] = "failed"
                report["fatal_error"] = f"{type(error).__name__}: {error}"
                write_json(args.report, report)
            raise


if __name__ == "__main__":
    raise SystemExit(main())
