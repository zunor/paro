# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Independent, bounded binary64 aggregate result contract.

No global epsilon: exact integer bags define high precision moments and the
enclosure of every Welford update / Chan merge schedule. This optional oracle
is only applicable to small exactly representable bags. All other domains
are Uncovered. It does not alter the default cross-engine exact comparator.
"""
from dataclasses import dataclass
from decimal import Decimal, localcontext
from functools import lru_cache
from fractions import Fraction
import math


VERSION = "integer-welford-schedules-v1"


class Uncovered(ValueError):
    pass


@dataclass(frozen=True)
class Interval:
    lower: float
    upper: float

    def contains(self, value):
        return (type(value) is float and math.isfinite(value)
                and self.lower <= value <= self.upper)

    def greater_than(self, threshold):
        if self.lower > threshold:
            return True
        if self.upper <= threshold:
            return False
        raise Uncovered("roundoff enclosure crosses a relational predicate boundary")


def sample_moments(values):
    """Exact integer sufficient statistics, independently evaluated at 100 digits."""
    values = tuple(value for value in values if value is not None)
    if any(type(value) is not int for value in values):
        raise Uncovered("integer input proof required")
    with localcontext() as ctx:
        ctx.prec = 100
        n = len(values)
        if not n:
            return None, None, None
        total = Decimal(sum(values))
        mean = total / n
        if n == 1:
            return mean, None, None
        m2 = Decimal(sum(v*v for v in values)) - total*total/n
        variance = m2 / (n-1)
        cv = variance.sqrt()/mean if mean else None
        return mean, variance, cv


def schedule_envelope(values):
    """All update orders and binary partition/merge trees, not measured ULPs.

    Six inputs bound exhaustive oracle work, not production search. Absolute
    integer sum <= 2**53 proves every AVG partial sum exact in binary64.
    Covers unfused Chan merge and the pinned DuckDB fused-mean merge.
    No overflow or extended precision is admitted.
    """
    values = tuple(v for v in values if v is not None)
    if any(type(v) is not int for v in values):
        raise Uncovered("integer input proof required")
    if len(values) > 6 or sum(abs(v) for v in values) > 2**53:
        raise Uncovered("outside bounded exact-input schedule domain")
    if not values:
        return None, None, None

    # Local exhaustive subproblem memoization is oracle-only and discarded
    # on return. No optimizer state or search choice is consulted.
    @lru_cache(None)
    def states(mask):
        if mask == 0:
            return frozenset([(0, 0.0, 0.0)])
        result = set()
        for i, value in enumerate(values):
            if mask & (1 << i):
                for n, mean, m2 in states(mask ^ (1 << i)):
                    delta = float(value) - mean
                    updated = mean + delta / (n+1)
                    result.add((n+1, updated, m2 + delta*(float(value)-updated)))
        left = (mask-1) & mask
        while left:
            right = mask ^ left
            for a, ma, sa in states(left):
                for b, mb, sb in states(right):
                    delta = mb-ma
                    result.add((a+b, ma+delta*(b/(a+b)),
                                sa+(sb+delta*delta*(float(a)*float(b)/float(a+b)))))
                    # Independent exact multiply-add rounded once: Python
                    # versions without math.fma can still enumerate the
                    # oracle's fused mean without host compiler dependence.
                    fused_mean = float(Fraction(b/(a+b))*Fraction(delta)+Fraction(ma))
                    result.add((a+b, fused_mean,
                                sb+sa+delta*delta*float(b)*float(a)/float(a+b)))
            left = (left-1) & mask
        return frozenset(result)

    n = len(values)
    mean = float(sum(values))/n
    if n == 1:
        return Interval(mean, mean), None, None
    variances = [m2/(n-1) for _, _, m2 in states((1 << n)-1)]
    if any(v < 0 or not math.isfinite(v) for v in variances):
        raise Uncovered("invalid variance arithmetic")
    deviation = [math.sqrt(v) for v in variances]
    cov = [s/mean for s in deviation] if mean else []
    return (Interval(mean, mean), Interval(min(variances), max(variances)),
            Interval(min(cov), max(cov)) if cov else None)


def assert_numeric_bijection(actual, reference):
    """Reference cells are exact values or explicitly admitted Intervals.

    Augmenting-path matching preserves duplicates and handles overlapping
    intervals without greedy reuse. NULL and non-approximate types are exact.
    A reference is independent oracle output, never an approximate Counter.
    """
    if len(actual) != len(reference):
        raise AssertionError("row multiplicity differs")

    def matches(row, expected):
        return len(row) == len(expected) and all(
            target.contains(value) if isinstance(target, Interval)
            else type(value) is type(target) and value == target
            for value, target in zip(row, expected))

    edges = [[i for i, expected in enumerate(reference) if matches(row, expected)]
             for row in actual]
    assigned = {}

    def augment(row, seen):
        for target in edges[row]:
            if target in seen:
                continue
            seen.add(target)
            if target not in assigned or augment(assigned[target], seen):
                assigned[target] = row
                return True
        return False

    for row in range(len(actual)):
        if not augment(row, set()):
            raise AssertionError(f"no one-to-one numeric match for row {row}")
