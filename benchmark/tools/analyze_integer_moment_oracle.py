#!/usr/bin/env python3
"""Independent diagnostic for grouped mean / sample coefficient of variation.

Consumes capture_result_difference artifacts. This never changes benchmark
acceptance: exact schema/bag failures remain failures. Integer sufficient
statistics are checked across engines before Decimal arithmetic is used.
Column roles are supplied explicitly, not guessed from physical node order.
"""

import argparse
from collections import Counter
from decimal import Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path
import struct


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def integer(cell):
    value = Decimal(cell["text"])
    if not value.is_finite() or value != value.to_integral_value():
        raise ValueError(f"non-integral sufficient statistic: {cell}")
    return int(value)


def ulps(a, b):
    if not math.isfinite(a) or not math.isfinite(b):
        raise ValueError("non-finite statistic is not covered by this diagnostic")
    def ordered(value):
        bits = struct.unpack(">Q", struct.pack(">d", value))[0]
        # Fold the finite number line around zero, identifying signed zeros.
        # Python's ~ has unbounded sign extension and is not a u64 complement.
        magnitude = bits & ((1 << 63) - 1)
        return (1 << 63) - magnitude if bits >> 63 else (1 << 63) + magnitude
    return abs(ordered(a) - ordered(b))


def analyze(moments, results, key_count, roles):
    """roles: [(key column indices, mean column index, cov column index)]."""
    actual = [tuple(map(integer, row)) for row in moments["actual"]]
    expected = [tuple(map(integer, row)) for row in moments["expected"]]
    if actual != expected:
        raise ValueError("integer sufficient statistics or their ordered keys differ")
    oracle = {}
    with localcontext() as context:
        context.prec = 80
        for row in actual:
            key, (n, total, squares) = row[:key_count], row[key_count:]
            if key in oracle:
                raise ValueError(f"duplicate aggregate key {key}")
            if n <= 1 or total == 0:
                # Unsupported scalar cases must not silently become zeros.
                oracle[key] = None
                continue
            n, total, squares = map(Decimal, (n, total, squares))
            mean = total / n
            variance = (squares - total * total / n) / (n - 1)
            if variance < 0:
                raise ValueError(f"negative exact variance for {key}")
            oracle[key] = (float(mean), float(variance.sqrt() / mean))
    report = {"integer_groups": len(oracle), "integer_values_equal": True,
              "decimal_precision": 80, "oracle": "integer_sample_moments",
              "engines": {}, "original_oracle_status": results["oracle_status"]}
    role_columns = {column for keys, mean, cov in roles for column in (*keys, mean, cov)}
    widths = {len(row) for row in results["actual"] + results["expected"]}
    if len(widths) != 1 or role_columns != set(range(next(iter(widths)))):
        raise ValueError("every result column must have an explicit role")
    keys_by_engine = {}
    for engine in ("actual", "expected"):
        counts, deviations, keys_list = Counter(), [], []
        for ordinal, row in enumerate(results[engine]):
            row_keys = []
            for keys, mean, cov in roles:
                key = tuple(integer(row[c]) for c in keys)
                row_keys.append(key)
                values = oracle[key]
                if values is None:
                    raise ValueError(f"selected key needs zero/NULL contract: {key}")
                for column, target in zip((mean, cov), values):
                    observed = float.fromhex(row[column]["float_hex"])
                    distance = ulps(observed, target)
                    counts[distance] += 1
                    if distance:
                        deviations.append({"row": ordinal, "column": column,
                            "key": key, "observed_hex": observed.hex(),
                            "oracle_hex": target.hex(), "ulp": distance})
            keys_list.append(tuple(row_keys))
        keys_by_engine[engine] = keys_list
        report["engines"][engine] = {"rows": len(keys_list),
            "ulp_histogram": dict(sorted(counts.items())), "deviations": deviations}
    if keys_by_engine["actual"] != keys_by_engine["expected"]:
        raise ValueError("result key order or multiset differs")
    report["exact_ordered_keys_equal"] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moments", type=Path, required=True)
    parser.add_argument("--result", type=Path, action="append", required=True)
    parser.add_argument("--key-count", type=int, required=True)
    parser.add_argument("--roles", required=True,
                        help='JSON [[[key_indices], mean_index, cov_index], ...]')
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    moments = json.loads(args.moments.read_text())["result_sets"][0]
    reports = []
    for path in args.result:
        data = json.loads(path.read_text())
        reports.append({"source": str(path), "sha256": digest(path),
                        "binary_sha256": data["binary_sha256"],
                        **analyze(moments, data["result_sets"][0], args.key_count,
                                  json.loads(args.roles))})
    args.output.write_text(json.dumps({"schema_version": 1,
        "moment_sha256": digest(args.moments), "reports": reports}, indent=2) + "\n")


if __name__ == "__main__":
    main()
