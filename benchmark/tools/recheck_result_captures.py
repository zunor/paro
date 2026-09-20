#!/usr/bin/env python3
"""Rebind immutable captures; never execute target SQL or overwrite raw evidence."""
import argparse
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
import duckdb
import _duckdb
from bound_result_contract import BoundResult, Uncovered, result_verdicts, catalog_from_rows, CATALOG_SQL
from tpcds_result_contract import ColumnContract


def decode(value):
    kind, text = value["python_type"], value["text"]
    if kind == "NoneType":
        return None
    if kind == "int":
        return int(text)
    if kind == "Decimal":
        return Decimal(text)
    if kind == "float":
        return float.fromhex(value["float_hex"])
    if kind == "str":
        return text
    if kind == "bool":
        if text not in {"True", "False"}:
            raise ValueError("invalid bool capture")
        return text == "True"
    if kind in {"date", "datetime", "time"}:
        return {"date": date, "datetime": datetime, "time": time}[kind].fromisoformat(text)
    raise Uncovered("capture scalar decoder: " + kind)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for flag in ("captures", "corpus", "database", "output", "source-manifest"):
        ap.add_argument("--"+flag, type=Path, required=True)
    args = ap.parse_args()
    if args.output.exists():
        raise ValueError("refuse to overwrite evidence")
    if duckdb.__version__ != "1.5.5":
        raise ValueError("pinned parser required")
    manifest = json.loads(args.source_manifest.read_text())
    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    for actual, expected in [(duckdb.__version__, manifest["duckdb_version"]),
        (digest(Path(_duckdb.__file__)), manifest["duckdb_extension_sha256"]),
        (digest(args.database), manifest["duckdb_database_sha256"])]:
        if actual != expected:
            raise ValueError("oracle identity mismatch")
    if {p.name for p in args.corpus.glob("*.sql")} != set(manifest["corpus_files"]):
        raise ValueError("corpus membership mismatch")
    expected_captures = {Path(n).stem for n in manifest["corpus_files"]}
    if {p.stem for p in args.captures.glob("[0-9][0-9].json")} != expected_captures:
        raise ValueError("missing/unexpected capture")
    with duckdb.connect(str(args.database), read_only=True) as parser:
        metadata = parser.execute(CATALOG_SQL).fetchall()
        catalog = catalog_from_rows(metadata)
        report = {"contract": "typed-result-v3", "source_manifest_sha256": digest(args.source_manifest),
                  "comparator_files": {p.name: digest(p) for p in [Path(__file__), *[Path(__file__).parents[1]/"corpora"/name
                    for name in ("bound_result_contract.py", "exact_result_value.py", "tpcds_result_contract.py")]]},
                  "catalog": metadata, "catalog_sha256": hashlib.sha256(json.dumps(metadata).encode()).hexdigest(), "queries": {}}
        for path in sorted(args.captures.glob("[0-9][0-9].json")):
            capture = json.loads(path.read_text())
            for field in ("seed_sha256", "duckdb_database_sha256", "duckdb_version", "duckdb_extension_sha256"):
                if capture.get(field) != manifest[field]:
                    raise ValueError("capture identity mismatch: " + field)
            if capture.get("binary_sha256") not in {manifest[a]["binary_sha256"] for a in ("control","probe")}:
                raise ValueError("unregistered binary")
            if capture.get("execution_error") or capture.get("server_observed_environment") is None:
                raise ValueError("execution/server observation is not certified")
            sql_path = args.corpus/(path.stem+".sql")
            sql = sql_path.read_text()
            if digest(sql_path) != capture["sql_sha256"] or digest(sql_path) != manifest["corpus_files"][sql_path.name]:
                raise ValueError("SQL/capture identity mismatch")
            statements = parser.extract_statements(sql)
            if len(statements) != len(capture["result_sets"]):
                raise ValueError("statement/result arity")
            results = []
            for statement, item in zip(statements, capture["result_sets"]):
                try:
                    bound = BoundResult(statement.query, parser, catalog)
                    results.append(result_verdicts(bound,
                        [tuple(decode(v) for v in row) for row in item["actual"]],
                        [ColumnContract(**c) for c in item["actual_schema"]],
                        [tuple(decode(v) for v in row) for row in item["expected"]],
                        [ColumnContract(**c) for c in item["expected_schema"]]))
                except (Uncovered, ValueError) as error:
                    results.append({k: {"status": "Uncovered", "error": str(error)} for k in ("identity", "schema", "bag", "order")})
            report["queries"][path.stem] = {"capture_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "checks": results}
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    for query, result in report["queries"].items():
        errors = [f"{name}: {c.get('error')}" for checks in result["checks"] for name,c in checks.items() if c["status"] != "pass"]
        if errors:
            print(query, json.dumps(errors))
    print("assessed", len(report["queries"]), "immutable captures")


if __name__ == "__main__":
    main()
