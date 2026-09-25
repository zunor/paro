#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Index raw correctness captures; never turn an uncovered difference into PASS."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def summarize(path):
    data = json.loads(path.read_text())
    sets = data.get("result_sets", [])
    if data.get("execution_error"):
        verdict = "execution_error"
    elif not sets:
        verdict = "uncovered_no_result"
    elif all(s.get("oracle_status") == "exact_match" for s in sets):
        verdict = "exact_match"
    else:
        verdict = "unresolved_difference"
    return {
        "capture": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "verdict": verdict, "sql_sha256": data.get("sql_sha256"),
        "binary_sha256": data.get("binary_sha256"),
        "result_contract_version": data.get("result_contract_version"),
        "server_observed": data.get("server_observed_environment") is not None,
        "stage": data.get("active_stage"), "sqlstate": data.get("sqlstate"),
        "execution_error": data.get("execution_error"),
        "sets": [{"ordinal": s["ordinal"], "actual_rows": len(s["actual"]),
                  "expected_rows": len(s["expected"]), "checks": s.get("checks", {}),
                  "error": s.get("error")} for s in sets],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--corpus", type=Path, required=True)
    args = parser.parse_args()
    rows = {p.stem: summarize(p) for p in sorted(args.directory.glob("*.json"))}
    expected = {p.stem for p in args.corpus.glob("*.sql")}
    print(json.dumps({"schema_version": 1,
        "counts": dict(Counter(r["verdict"] for r in rows.values())),
        "missing": sorted(expected - rows.keys()),
        "unexpected": sorted(rows.keys() - expected), "queries": rows}, indent=2))


if __name__ == "__main__":
    main()
