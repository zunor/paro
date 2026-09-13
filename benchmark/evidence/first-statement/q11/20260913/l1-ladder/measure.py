#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0
"""L1-LADDER measurement artifact. Never builds; imports a supplied clean runtime.

Do not invoke during another campaign. See PREREGISTRATION.md before running.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
from types import SimpleNamespace

BLOCKS = 36
RUNGS = tuple(f"L{i}" for i in range(8))
E2_SQL = "benchmark/evidence/first-statement/q11/20260913/e2-decouple/pre-touch.sql"
QUERIES = {
    "L0": "SELECT count(*) AS value FROM store_sales;\n",
    "L1": "SELECT count(ss_customer_sk) AS value FROM store_sales;\n",
    "L2": "SELECT sum(ss_ext_list_price) AS value FROM store_sales;\n",
    "L3": "SELECT sum(ss_ext_list_price - ss_ext_discount_amt) AS value FROM store_sales;\n",
    "L5": "SELECT ss_customer_sk AS customer_key, sum(ss_ext_list_price - ss_ext_discount_amt) AS value FROM store_sales GROUP BY ss_customer_sk;\n",
    "L6": "SELECT count(c_email_address) AS value FROM customer;\n",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sql_digest(query):
    return hashlib.sha256(query.encode("utf-8")).hexdigest()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-repo", type=Path, required=True)
    parser.add_argument("--expected-commit", required=True, help="Full reviewed clean runtime HEAD")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-attestation", type=Path, required=True,
                        help="Existing helper build attestation, or report containing build_attestation")
    parser.add_argument("--server-data-dir", type=Path, required=True, help="Offline immutable seed")
    parser.add_argument("--duckdb-database", type=Path, required=True)
    parser.add_argument("--dataset-source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True, help="New directory outside both repos/seeds")
    parser.add_argument("--rungs", nargs="+", choices=RUNGS, default=["L0"])
    parser.add_argument("--cohort", choices=("ladder", "diagnostic"), default="ladder")
    parser.add_argument("--capture-plan", action="store_true", help="Plain EXPLAIN after diagnostic samples only")
    parser.add_argument("--pilot-report", type=Path)
    parser.add_argument("--pilot-review", type=Path)
    parser.add_argument("--pilot-stop-ratio", type=float, required=True,
                        help="Registered operational L0 W P/D trigger: pass 4")
    parser.add_argument("--metadata-track", choices=("none", "generator-declared"), required=True)
    parser.add_argument("--listen", default="127.0.0.1:6432")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="paro")
    parser.add_argument("--threads", type=int, choices=(4,), default=4)
    parser.add_argument("--memory-limit", choices=("2GB",), default="2GB")
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument("--random-seed", type=int, default=1)
    args = parser.parse_args()
    require(len(args.rungs) == len(set(args.rungs)), "Duplicate rung selection")
    require("L0" not in args.rungs or args.rungs == ["L0"], "Run and review L0 separately")
    require(math.isfinite(args.pilot_stop_ratio) and args.pilot_stop_ratio > 0, "Invalid stop ratio")
    require(not args.capture_plan or args.cohort == "diagnostic", "Plan capture requires separate diagnostic cohort")
    return args


def runtime(args):
    repo = args.runtime_repo.resolve()
    sys.dont_write_bytecode = True
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    sys.path.insert(0, str(repo / "benchmark/corpora"))
    modules = [importlib.import_module(name) for name in
               ("benchmark_evidence", "tpcds_compare", "tpcds_result_contract")]
    for module in modules:
        require(Path(module.__file__).resolve().is_relative_to(repo / "benchmark/corpora"),
                "Helper imported from outside the clean runtime")
    return SimpleNamespace(e=modules[0], c=modules[1], r=modules[2])


def file_identity(rt, path):
    path = path.resolve()
    return {"path": str(path), "sha256": rt.e.content_digest(path)}


def source_digest(rt, repo):
    # Commit identifies all tracked content; hash engine/build inputs as well.
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=repo).split(b"\0")
    digest = hashlib.sha256()
    for raw in sorted(path for path in paths if path):
        path = repo / os.fsdecode(raw)
        if path.suffix in (".rs", ".toml", ".lock") or path.name == "rust-toolchain":
            digest.update(raw + b"\0" + bytes.fromhex(rt.e.content_digest(path)))
    return digest.hexdigest()


def identities(rt, args):
    repo = args.runtime_repo.resolve()
    source = rt.e.repository_identity(repo)
    require(source["dirty"] is False and source["commit"] == args.expected_commit,
            "Runtime must be the reviewed clean commit; never build from this driver")
    document = read_json(args.build_attestation)
    build = document.get("build_attestation", document)
    require(build["source"] == source, "Build attestation does not identify current clean source")
    require(rt.e.content_digest(args.binary) == build["binary_sha256"], "Binary differs from attested image")
    require(args.binary.is_file() and os.access(args.binary, os.X_OK), "Binary is not executable")
    driver = Path(__file__).resolve()
    helper_paths = sorted((repo / "benchmark/corpora").glob("*.py"))
    result = {
        "source": source, "engine_build_input_sha256": source_digest(rt, repo),
        "build_attestation": build, "attestation_file": file_identity(rt, args.build_attestation),
        "binary": file_identity(rt, args.binary),
        "harness": [file_identity(rt, path) for path in helper_paths],
        "driver": file_identity(rt, driver),
        "preregistration": file_identity(rt, driver.with_name("PREREGISTRATION.md")),
        "duckdb_database": file_identity(rt, args.duckdb_database),
        "dataset_source_sha256": rt.e.tree_digest(args.dataset_source_dir),
        "e2_sql": file_identity(rt, repo / E2_SQL),
        "python": sys.version, "platform": platform.platform(),
        "duckdb_version": rt.c.duckdb.__version__,
        "duckdb_extension": rt.c.extension_digest(rt.c._duckdb),
        "psycopg_version": rt.c.psycopg.__version__,
    }
    return result


def configuration(args):
    return {"threads": args.threads, "memory_limit": args.memory_limit,
            "metadata_track": args.metadata_track, "database": args.database,
            "paro_result_format": "binary", "optimizer_verify": True,
            "handoff": "1", "compile_work": "1", "cold_work": "1",
            "timeout_seconds": args.statement_timeout_seconds,
            "random_seed": args.random_seed, "pilot_stop_ratio": args.pilot_stop_ratio}


def pilot_gate(rt, args, identity, seed):
    if args.rungs == ["L0"]:
        return None
    require(args.pilot_report and args.pilot_review, "Later rungs require L0 report and explicit review")
    pilot, review = read_json(args.pilot_report), read_json(args.pilot_review)
    require(pilot["status"] == "complete" and pilot["cohort"] == "ladder"
            and pilot["selected_rungs"] == ["L0"] and len(pilot["rungs"]["L0"]["blocks"]) == BLOCKS,
            "Pilot must be a complete eligible 36-block L0 cohort")
    require(pilot["identity"] == identity and pilot["configuration"] == configuration(args),
            "Pilot and ladder source/build/harness/data/configuration must match exactly")
    require(pilot["seed"] == {"path": str(seed.path), "sha256": seed.sha256}, "Pilot Paro seed differs")
    require(review.get("pilot_report_sha256") == rt.e.content_digest(args.pilot_report)
            and review.get("decision") == "continue" and review.get("reviewer") and review.get("reason"),
            "Missing explicit review tied to this exact pilot")
    if pilot["pilot_stop_triggered"]:
        require(review.get("foundation_causal") is False and review.get("evidence"),
                "Triggered pilot stops the ladder unless reviewed evidence shows non-foundational attribution")
    evidence = []
    for item in review.get("evidence", []):
        require(file_identity(rt, Path(item["path"])) == item, "Pilot-review evidence changed")
        evidence.append(item)
    return {"pilot": file_identity(rt, args.pilot_report),
            "review": file_identity(rt, args.pilot_review), "decision": review, "evidence": evidence}


def query_specs(rt, args):
    queries = dict(QUERIES)
    queries["L7"] = (args.runtime_repo.resolve() / E2_SQL).read_text(encoding="utf-8")
    bounds = None
    if "L4" in args.rungs:
        bound_sql = ("SELECT min(d_date_sk) AS a, max(d_date_sk) AS b, count(*) AS n, "
                     "count(DISTINCT d_date_sk) AS ndv FROM date_dim WHERE d_year BETWEEN 2001 AND 2002;")
        # Separate read-only preparation process, never a measured process.
        with rt.c.DuckDBProcess(args.duckdb_database, args.threads, args.memory_limit) as duck:
            rows, schema, _ = duck.execute(bound_sql)
            require(len(rows) == 1 and rows[0][0] is not None and rows[0][1] is not None, "Missing date bounds")
            a, b, count, distinct = map(int, rows[0])
            require(a <= b and count == distinct == b - a + 1, "Date range is not unique and contiguous")
            check_sql = (f"SELECT count(*) AS n FROM date_dim WHERE "
                         f"(d_date_sk BETWEEN {a} AND {b} AND (d_year IS NULL OR d_year NOT BETWEEN 2001 AND 2002)) "
                         f"OR (d_year BETWEEN 2001 AND 2002 AND (d_date_sk IS NULL OR d_date_sk NOT BETWEEN {a} AND {b}));")
            check, _, _ = duck.execute(check_sql)
            require(check == [(0,)], "Date-key BETWEEN is not equivalent to the declared year domain")
            bounds = {"a": a, "b": b, "sql": bound_sql, "check_sql": check_sql,
                      "rows": [list(rows[0])], "schema": rt.c.schema_report(schema),
                      "check_rows": check, "process": duck.identity,
                      "database": file_identity(rt, args.duckdb_database)}
        queries["L4"] = f"SELECT count(*) AS value FROM store_sales WHERE ss_sold_date_sk BETWEEN {a} AND {b};\n"
    specs = {}
    for rung in args.rungs:
        query = queries[rung]
        statements = rt.c.duckdb.extract_statements(query)
        require(len(statements) == 1 and statements[0].type == rt.c.duckdb.StatementType.SELECT,
                "Rungs must each be one read-only SELECT")
        require(not rt.r.parse_order_contract(query, ()), "Ladder requires unordered exact bags")
        specs[rung] = {"sql": query, "sha256": sql_digest(query),
                       "query_fingerprint": rt.e.statement_fingerprint(query)}
    return specs, bounds


def cache_and_fill(rt, connection, query, occurrence, previous, sample):
    evidence = rt.c.collect_statement_cache_evidence(connection, query, expected_occurrence=occurrence)
    sample["cache_evidence"] = evidence  # Retain even an ineligible observation.
    require(evidence.get("status") == "verified" and evidence.get("occurrence") == occurrence
            and evidence.get("cache_hit") is (occurrence == 1)
            and evidence.get("query_fingerprint") == rt.e.statement_fingerprint(query),
            f"Actual occurrence {occurrence} cache evidence missing or incorrect: {evidence}")
    work = evidence.get("execution_work", {})
    metrics = work.get("metrics", {})
    require(work.get("query_fingerprint") == rt.e.statement_fingerprint(query)
            and isinstance(work.get("execution_id"), int)
            and metrics.get("valid_isolated_window") == 1
            and metrics.get("operator_overflow_count") == 0
            and "image_id" in metrics and "buffer_fill_count" in metrics
            and "buffer_fill_input_bytes" in metrics, "Incomplete/invalid E1 occurrence evidence")
    if occurrence == 1:
        old = previous["execution_work"]
        require(work["execution_id"] > old["execution_id"]
                and metrics["image_id"] == old["metrics"]["image_id"], "Second occurrence did not reuse the first image")
        require(metrics["buffer_fill_count"] == metrics["buffer_fill_input_bytes"] == 0,
                "Second occurrence is not zero-fill; retain failed block, do not replace it")
    return evidence


def sample_block(rt, args, seed, rung, spec, block, order, record):
    query = spec["sql"]
    log = args.output_dir / f"{rung}.{args.cohort}.block{block:03d}.parod.log"
    sample_id = f"ladder.{rung}.{args.cohort}.{block}"
    env = {name: None for name in os.environ if name.startswith("PARO_") or name == "RUST_LOG"}
    env.update({"PARO_STATEMENT_CACHE_EVIDENCE": "1", "PARO_COLD_WORK_EVIDENCE": "1",
                "PARO_COMPILE_WORK_EVIDENCE": "1", "PARO_QUALITY_POLICY_HANDOFF": "1",
                "PARO_STATEMENT_TRACE": "1" if args.cohort == "diagnostic" else None,
                "PARO_STATEMENT_TRACE_SAMPLE": sample_id if args.cohort == "diagnostic" else None})
    record.update({"block": block, "order": order, "samples": {"paro": [], "duckdb": []},
                   "environment_overrides": env, "status": "started"})
    with ExitStack() as stack:
        server = stack.enter_context(rt.e.isolated_paro_server(
            args.binary, seed, args.listen, log, max_memory=args.memory_limit, threads=args.threads,
            statement_trace=args.cohort == "diagnostic", trace_sample_id=sample_id,
            cache_evidence=True, optimizer_environment=env))
        duck = stack.enter_context(rt.c.DuckDBProcess(args.duckdb_database, args.threads, args.memory_limit))
        paro = rt.c.open_paro_connection(args)
        stack.callback(paro.close)
        record["processes"] = {"paro": server.identity(), "duckdb": duck.identity}
        inventories = {"paro": rt.c.paro_metadata_inventory(paro), "duckdb": rt.c.duckdb_metadata_inventory(duck)}
        record["metadata"] = inventories
        record["metadata_symmetric"] = rt.c.validate_metadata_track(
            args.metadata_track, inventories["paro"], inventories["duckdb"])
        canonical, schemas = {}, {}
        for name in order:
            engine = "paro" if name == "A" else "duckdb"
            occurrence = len(record["samples"][engine])
            rows, schema, elapsed = (rt.c.timed_run_paro(paro, query, True)
                                     if engine == "paro" else duck.execute(query))
            require(elapsed > 0 and math.isfinite(elapsed), "Invalid timer value")
            sample = {"occurrence": occurrence, "execute_fetch_ms": elapsed,
                      "row_count": len(rows), "schema": rt.c.schema_report(schema)}
            record["samples"][engine].append(sample)
            if engine == "paro":
                previous = record["samples"][engine][0].get("cache_evidence")
                cache_and_fill(rt, paro, query, occurrence, previous, sample)
            normalized = rt.r.canonicalize_rows(rows, schema)
            sample["typed_bag_sha256"] = rt.r.multiset_digest(normalized)
            if engine in canonical:
                rt.r.assert_compatible_schema(schemas[engine], schema)
                rt.r.assert_same_multiset(canonical[engine], normalized)
            else:
                canonical[engine], schemas[engine] = normalized, schema
            if len(canonical) == 2:
                rt.r.assert_compatible_schema(schemas["paro"], schemas["duckdb"])
                rt.r.assert_same_multiset(canonical["paro"], canonical["duckdb"])
        require(all(len(samples) == 2 for samples in record["samples"].values()), "Exactly two executions required")
        record["full_typed_bag_validation"] = "passed_all_four_results"
        if args.capture_plan:
            # Plain EXPLAIN only, after both occurrences and evidence reads.
            record["plans"] = {}
            for engine in ("paro", "duckdb"):
                rows, _, _ = (rt.c.timed_run_paro(paro, "EXPLAIN " + query, False)
                              if engine == "paro" else duck.execute("EXPLAIN " + query))
                record["plans"][engine] = [[str(cell) for cell in row] for row in rows]
    if args.cohort == "diagnostic":
        traces = [trace for trace in rt.e.parse_statement_trace_log(log)
                  if trace["query_fingerprint"] == spec["query_fingerprint"]]
        require(len(traces) == 2, "Diagnostic must retain exactly two actual target traces")
        for occurrence, trace in enumerate(traces):
            rt.e.validate_statement_trace(trace, expected_process_id=record["processes"]["paro"]["pid"],
                expected_sample_id=sample_id, expected_query_fingerprint=spec["query_fingerprint"],
                require_complete=occurrence == 0)
            require(trace["events"][-1]["event"] == "statement_complete", "Incomplete target trace")
        record["target_statement_traces"] = traces
    record["log"] = file_identity(rt, log)
    record["status"] = "passed"


def summarize(rt, blocks):
    result = {}
    for occurrence in (0, 1):
        # Existing helper performs whole-process-pair bootstrap for ONE P/D
        # observation per block. Never feed C1 and W into one warm ABBA pool.
        pairs = [{"cold_statement_ms": {engine: block["samples"][engine][occurrence]["execute_fetch_ms"]
                                       for engine in ("paro", "duckdb")}} for block in blocks]
        result[f"occurrence_{occurrence}"] = rt.c.hierarchical_cold_ratio(pairs, 10_000)
        result[f"occurrence_{occurrence}"]["timings"] = {
            engine: rt.c.timing_summary([pair["cold_statement_ms"][engine] for pair in pairs])
            for engine in ("paro", "duckdb")}
    return result


def main():
    args = arguments()
    args.runtime_repo, args.output_dir = args.runtime_repo.resolve(), args.output_dir.resolve()
    rt = runtime(args)
    for protected in (args.runtime_repo, Path(__file__).resolve().parents[6],
                      args.server_data_dir.resolve(), args.dataset_source_dir.resolve()):
        require(not args.output_dir.is_relative_to(protected), "Output must be outside repos and input directories")
    require(not args.output_dir.exists(), "Refuse to overwrite or resume a campaign directory")
    identity = identities(rt, args)
    seed = rt.e.ImmutableDataSeed.capture(args.server_data_dir)
    gate = pilot_gate(rt, args, identity, seed)
    specs, bounds = query_specs(rt, args)
    args.output_dir.mkdir(parents=True)
    for rung, spec in specs.items():
        (args.output_dir / f"{rung}.sql").write_text(spec["sql"], encoding="utf-8")
    report = {"schema_version": 1, "status": "running", "cohort": args.cohort,
              "selected_rungs": args.rungs, "identity": identity, "configuration": configuration(args),
              "seed": {"path": str(seed.path), "sha256": seed.sha256}, "pilot_review": gate,
              "date_bounds": bounds, "rungs": {}, "normal_e1_off_timing": False,
              "e1_overhead": "unquantified; no normal parity or scan-throughput causal claim",
              "os_cache_policy": "not flushed; private Paro snapshot, read-only DuckDB, process-fresh only"}
    path = args.output_dir / "report.json"
    try:
        for rung, spec in specs.items():
            count = BLOCKS if args.cohort == "ladder" else 1
            orders = ["ABBA", "BAAB"] * (BLOCKS // 2)
            random.Random(args.random_seed).shuffle(orders)
            entry = {"query": spec, "blocks": []}
            report["rungs"][rung] = entry
            for block, order in enumerate(orders[:count]):
                record = {}
                entry["blocks"].append(record)
                rt.c.write_report(path, report)
                sample_block(rt, args, seed, rung, spec, block, order, record)
                rt.c.write_report(path, report)
            if args.cohort == "ladder":
                entry["summary"] = summarize(rt, entry["blocks"])
            if rung == "L0" and args.cohort == "ladder":
                report["pilot_ratio_unrounded"] = statistics.geometric_mean([
                    block["samples"]["paro"][1]["execute_fetch_ms"]
                    / block["samples"]["duckdb"][1]["execute_fetch_ms"]
                    for block in entry["blocks"]])
                report["pilot_stop_triggered"] = report["pilot_ratio_unrounded"] >= args.pilot_stop_ratio
                report["next_action"] = "STOP_FOR_PILOT_REVIEW; no later rung runs automatically"
        seed.verify_unchanged()
        require(identities(rt, args) == identity, "Source/build/harness/data/SQL identity changed")
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "failed_no_replacement_samples"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        rt.c.write_report(path, report)


if __name__ == "__main__":
    main()
