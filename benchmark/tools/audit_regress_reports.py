#!/usr/bin/env python3
"""Audit every failed regress transcript block without blessing snapshots."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(repository, control, probe):
    sys.path.insert(0, str(repository / "regress"))
    from harness.comparator import parse_result_file, _compare_one_block

    def cases(report):
        return set(re.findall(r"\[SCRIPT   FILE\]: (.*)", (report / "error.txt").read_text()))

    c, p = cases(control), cases(probe)
    unexpected = {}
    for arm, report, declared in [("control", control, c), ("probe", probe, p)]:
        names = {"_".join(Path(case).parts[1:]) + ".actual" for case in declared}
        unexpected[arm] = sorted(path.name for path in (report / "actuals").glob("*.actual") if path.name not in names)
    records = []
    categories = Counter()
    for case in sorted(c | p):
        relative = Path(case)
        expected = repository / "regress" / relative.with_suffix(".result")
        expected_blocks = parse_result_file(expected)
        record = {"case": case, "expected_sha256": sha(expected), "arms": {}}
        artifacts = []
        for arm, report in [("control", control), ("probe", probe)]:
            actual = report / "actuals" / ("_".join(relative.parts[1:]) + ".actual")
            if not actual.exists():
                record["arms"][arm] = {"missing_actual": True}
                continue
            artifacts.append(actual)
            blocks = parse_result_file(actual)
            differences = []
            for index, (x, y) in enumerate(zip(expected_blocks, blocks), 1):
                if _compare_one_block(index, x, y) is not None:
                    category = "explain_snapshot" if x.sql.lstrip().upper().startswith("EXPLAIN") else "non_explain_result"
                    categories[arm + ":" + category] += 1
                    differences.append({"block": index, "sql": x.sql, "category": category,
                        "actual_contains_error": any("ERROR" in line for line in y.raw_result_lines)})
            record["arms"][arm] = {"actual_sha256": sha(actual), "expected_blocks": len(expected_blocks),
                "actual_blocks": len(blocks), "differences": differences}
        record["actuals_byte_identical"] = len(artifacts) == 2 and artifacts[0].read_bytes() == artifacts[1].read_bytes()
        records.append(record)
    return {"schema_version": 1, "control_report": str(control), "probe_report": str(probe),
        "control_only_failures": sorted(c-p), "probe_only_failures": sorted(p-c),
        "unexpected_actuals": unexpected,
        "all_actuals_byte_identical": c == p and not any(unexpected.values()) and all(r["actuals_byte_identical"] for r in records),
        "counts": dict(categories), "cases": records,
        "acceptance": "raw snapshots remain failed; categories require explicit adjudication, not automatic acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--single", type=Path)
    for name in ["control", "probe"]:
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    if args.single:
        if args.control or args.probe:
            parser.error("single report is not a paired experiment")
        report = audit(args.repository, args.single, args.single)
        print(json.dumps({"schema_version": 1, "mode": "single_run_expected_comparison",
            "report": str(args.single), "unexpected_actuals": report["unexpected_actuals"]["probe"],
            "cases": [{"case": c["case"], "expected_sha256": c["expected_sha256"],
                       "actual": c["arms"]["probe"]} for c in report["cases"]],
            "acceptance": report["acceptance"]}, indent=2))
        return
    if not args.control or not args.probe:
        parser.error("provide --single or both --control and --probe")
    print(json.dumps(audit(args.repository, args.control, args.probe), indent=2))


if __name__ == "__main__":
    main()
