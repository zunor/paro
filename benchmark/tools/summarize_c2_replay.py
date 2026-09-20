#!/usr/bin/env python3
"""Join exact replay verdicts to separately hash-bound numerical certificates.

This generates an evidence ledger, not a comparator exception. Raw exact
failures remain in every record. No query number or SQL hash is whitelisted.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, action="append", required=True)
    parser.add_argument("--certificate", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("refuse to overwrite evidence")
    certificates = {}
    for path in args.certificate:
        cert = json.loads(path.read_text())
        for entry in cert["manifest"].values():
            if digest(entry["path"]) != entry["sha256"]:
                raise ValueError("certificate input identity mismatch")
        if cert["contract"] != "integer-welford-schedules-v1" or not all(
            cert["engines"][engine][key] == "pass"
            for engine in ("actual", "expected")
            for key in ("relational_bag_and_order", "numeric_schedule_enclosure")
        ):
            raise ValueError("unsupported/incomplete numerical certificate")
        certificates[cert["manifest"]["result"]["sha256"]] = {
            "path": str(path), "sha256": digest(path), "contract": cert["contract"]}
    ledger = {"contract": None, "performance_admitted": False, "replays": []}
    for path in args.replay:
        report = json.loads(path.read_text())
        if ledger["contract"] not in {None, report["contract"]}:
            raise ValueError("cannot combine different result contracts")
        ledger["contract"] = report["contract"]
        counts = Counter()
        for result in report["queries"].values():
            failures = [(name, value["status"]) for checks in result["checks"]
                        for name, value in checks.items() if value["status"] != "pass"]
            certificate = certificates.get(result["capture_sha256"])
            if not failures:
                status = "exact"
            elif certificate and all(name in {"bag", "order"} for name, _ in failures):
                status = "independently_bounded"
                result["independent_certificate"] = certificate
            elif all(status == "Uncovered" for _, status in failures):
                status = "Uncovered"
            else:
                status = "failed"
            result["adjudication"] = status
            counts[status] += 1
        ledger["replays"].append({"path": str(path), "sha256": digest(path),
                                  "counts": dict(counts), "verdicts": report})
    ledger["corpus_gate_closed"] = all(not any(r["counts"].get(k, 0)
        for k in ("Uncovered", "failed")) for r in ledger["replays"])
    args.output.write_text(json.dumps(ledger, indent=2)+"\n")
    print(json.dumps([r["counts"] for r in ledger["replays"]]))


if __name__ == "__main__":
    main()
