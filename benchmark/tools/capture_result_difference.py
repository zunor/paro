#!/usr/bin/env python3
"""Capture complete, typed result evidence without relaxing the corpus oracle.

This is a correctness diagnostic, never a performance sample. The supplied
harness is frozen independently of the server under test so parent/probe runs
can use the same oracle and server-observed environment check.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import importlib.util
import json
import os
from pathlib import Path
import sys


def encode_value(value):
    return {
        "python_type": type(value).__name__,
        "text": str(value),
        "repr": repr(value),
        "float_hex": value.hex() if isinstance(value, float) else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:16439")
    parser.add_argument("--verify", choices=("on", "off"), default="off")
    parser.add_argument("--handoff", choices=("on", "off"), default="on")
    args = parser.parse_args()
    sys.path.insert(0, str(args.harness.resolve() / "corpora"))
    from benchmark_evidence import (
        ImmutableDataSeed, content_digest, isolated_paro_server,
    )
    from tpcds_compare import (
        expected_server_diagnostic_environment, open_paro_connection,
        optimizer_evidence_environment, load_duckdb_runtime_manifest,
    )
    # Server lifecycle stays frozen independently, but result acceptance is
    # explicitly versioned by this checkout. Do not accidentally consume an
    # older contract already imported by the external lifecycle helper.
    contract_path = Path(__file__).resolve().parents[1] / "corpora/tpcds_result_contract.py"
    sys.path.insert(0, str(contract_path.parent))
    spec = importlib.util.spec_from_file_location("capture_result_contract", contract_path)
    contract = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = contract
    spec.loader.exec_module(contract)
    # BoundResult's lazy imports must use this contract too, even when the
    # independently pinned lifecycle harness imported an older module first.
    sys.modules["tpcds_result_contract"] = contract
    duckdb_schema, paro_schema = contract.duckdb_schema, contract.paro_schema
    import duckdb
    import _duckdb
    duckdb_runtime = load_duckdb_runtime_manifest(args.harness.resolve().parent)

    for name in list(os.environ):
        if name.startswith("PARO_"):
            del os.environ[name]
    if args.handoff == "on":
        os.environ["PARO_QUALITY_POLICY_HANDOFF"] = "1"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    seed = ImmutableDataSeed.capture(args.seed)
    query = args.sql.read_text()
    config = argparse.Namespace(
        listen=args.listen, database="postgres", user="paro", threads=4,
        memory_limit="2GB", statement_timeout_seconds=300,
        paro_optimizer_verify=args.verify,
    )
    report = {
        "schema_version": 2, "measurement_mode": "correctness_diagnostic",
        "result_contract_version": contract.RESULT_CONTRACT_VERSION,
        "result_contract_sha256": content_digest(contract_path),
        "binary_sha256": content_digest(args.binary),
        "seed_sha256": seed.sha256, "sql_sha256": content_digest(args.sql),
        "duckdb_version": duckdb.__version__,
        "duckdb_runtime_contract": duckdb_runtime,
        "duckdb_extension_sha256": content_digest(Path(_duckdb.__file__)),
        "duckdb_database_sha256": content_digest(args.duckdb),
        "optimizer_verify": args.verify, "handoff": args.handoff,
        "default_null_order": "NULLS_LAST_ON_ASC_FIRST_ON_DESC",
        "harness_files": {p.name: content_digest(p) for p in
            sorted((args.harness / "corpora").glob("*.py"))},
        "result_sets": [],
    }
    # Both engines receive the exact same SQL. Retain all result sets rather
    # than silently accepting only the first result of a compound query.
    try:
        with isolated_paro_server(
            args.binary, seed, args.listen, args.output.with_suffix(".parod.log"),
            max_memory="2GB", threads=4,
            optimizer_environment=optimizer_evidence_environment(),
        ) as server:
            connection, observed = open_paro_connection(
                config, expected_environment=expected_server_diagnostic_environment(
                    statement_trace=False, trace_sample_id=None, cache_evidence=False,
                ),
            )
            report["server_observed_environment"] = observed
            report["server"] = server.identity()
            with connection, duckdb.connect(str(args.duckdb), read_only=True) as oracle:
                oracle.execute("SET threads=4")
                oracle.execute("SET memory_limit='2GB'")
                oracle.execute("SET default_null_order='NULLS_LAST_ON_ASC_FIRST_ON_DESC'")
                statements = oracle.extract_statements(query)
                with connection.cursor(binary=True) as cursor:
                    # psycopg extended protocol accepts one statement at a time.
                    for ordinal, statement in enumerate(statements):
                        sql = statement.query
                        report["active_stage"] = {"ordinal": ordinal, "engine": "duckdb", "operation": "execute"}
                        oracle.execute(sql)
                        expected_rows = oracle.fetchall()
                        expected_schema = duckdb_schema(oracle.description)
                        report["active_stage"] = {"ordinal": ordinal, "engine": "paro", "operation": "execute"}
                        cursor.execute(sql)
                        actual_rows = cursor.fetchall()
                        actual_schema = paro_schema(cursor.description)
                        report["active_stage"]["operation"] = "compare"
                        item = {
                            "ordinal": ordinal,
                            "actual_schema": [asdict(c) for c in actual_schema],
                            "expected_schema": [asdict(c) for c in expected_schema],
                            "actual": [[encode_value(v) for v in r] for r in actual_rows],
                            "expected": [[encode_value(v) for v in r] for r in expected_rows],
                        }
                        from bound_result_contract import BoundResult, CATALOG_SQL, catalog_from_rows, result_verdicts
                        metadata = oracle.execute(CATALOG_SQL).fetchall()
                        bound = BoundResult(sql, oracle, catalog_from_rows(metadata))
                        item["checks"] = result_verdicts(bound, actual_rows, actual_schema,
                                                        expected_rows, expected_schema)
                        failures = [name + ": " + result["error"]
                                    for name, result in item["checks"].items()
                                    if result["status"] != "pass"]
                        item["oracle_status"] = "mismatch" if failures else "exact_match"
                        if failures:
                            item["error"] = "\n".join(failures)
                        report["result_sets"].append(item)
    except Exception as error:
        report["execution_error"] = f"{type(error).__name__}: {error}"
        report["sqlstate"] = getattr(error, "sqlstate", None)
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "results": [
        {"ordinal": r["ordinal"], "actual_rows": len(r["actual"]),
         "expected_rows": len(r["expected"]), "status": r["oracle_status"],
         "error": r.get("error")} for r in report["result_sets"]]}))


if __name__ == "__main__":
    main()
