# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Standalone contract oracle; standard library only, NOT EXECUTED at delivery.

This is an independent finite model, not a Paro integration test or a timing
model. Mandatory work has already completed in every example. All tuples are
new, resource-feasible, and use the same recipe/goal with fixed exact costs.
Integer scalar costs embed a valid dominance case: all other axes are equal.
Run only after the owner releases the performance window:
    python3 benchmark/evidence/first-statement/q11/20260913/ubatch/contract_oracle.py
"""

from dataclasses import asdict, dataclass
import json
import unittest


@dataclass(frozen=True)
class TupleCost:
    event: str
    cost: int


@dataclass(frozen=True)
class Outcome:
    consumed: int
    synthesized: tuple[str, ...]
    published: tuple[str, ...]
    rejected: tuple[str, ...]
    omitted: tuple[str, ...]
    incumbent: int


def per_tuple(tuples, credit, stop_after_publications=None):
    """Admit -> synthesize -> compare/publish -> possible yield, in order."""
    incumbent = 100  # mandatory baseline already published, outside credit
    synthesized, published, rejected, omitted = [], [], [], []
    for index, candidate in enumerate(tuples):
        if len(synthesized) == credit:
            omitted = [item.event for item in tuples[index:]]
            break
        synthesized.append(candidate.event)
        if candidate.cost < incumbent:
            incumbent = candidate.cost
            published.append(candidate.event)
        else:
            rejected.append(candidate.event)
        if (stop_after_publications is not None
                and len(published) >= stop_after_publications):
            omitted = [item.event for item in tuples[index + 1:]]
            break
    return Outcome(len(synthesized), tuple(synthesized), tuple(published),
                   tuple(rejected), tuple(omitted), incumbent)


def atomic_expression(tuples, credit):
    """Whole new optional domain is admitted or rejected as one unit batch.

The model deliberately grants identical sequential comparisons after successful
admission. Thus the budget counterexample does not depend on a worse comparator.
Intermediate results are externally visible only when this function returns.
"""
    if len(tuples) > credit:
        return Outcome(0, (), (), (), tuple(item.event for item in tuples), 100)
    return per_tuple(tuples, credit)


class ContractOracle(unittest.TestCase):
    def test_optional_credit_one_two_new_tuples_after_baseline(self):
        tuples = (TupleCost("a", 10), TupleCost("b", 5))
        sequential = per_tuple(tuples, credit=1)
        atomic = atomic_expression(tuples, credit=1)
        self.assertEqual(sequential, Outcome(1, ("a",), ("a",), (), ("b",), 10))
        self.assertEqual(atomic, Outcome(0, (), (), (), ("a", "b"), 100))
        self.assertNotEqual(sequential, atomic)

    def test_bounded_new_domains_have_prefix_not_atomic_semantics(self):
        for length in range(1, 5):
            tuples = tuple(TupleCost(str(i), 50 - i) for i in range(length))
            for credit in range(5):
                sequential = per_tuple(tuples, credit)
                atomic = atomic_expression(tuples, credit)
                self.assertEqual(sequential.consumed, min(length, credit))
                self.assertEqual(atomic.consumed, length if length <= credit else 0)
                if 0 < credit < length:
                    self.assertNotEqual(sequential, atomic)
                else:
                    self.assertEqual(sequential, atomic)

    def test_ample_budget_does_not_remove_publication_boundary(self):
        tuples = (TupleCost("a", 10), TupleCost("b", 5))
        at_yield = per_tuple(tuples, credit=2, stop_after_publications=1)
        whole = atomic_expression(tuples, credit=2)
        self.assertEqual(at_yield.synthesized, ("a",))
        self.assertEqual(at_yield.published, ("a",))
        self.assertEqual(at_yield.omitted, ("b",))
        self.assertEqual(whole.synthesized, ("a", "b"))
        self.assertNotEqual(at_yield, whole)
        # A later continuation can converge, but that does not equal the prefix.
        self.assertEqual(per_tuple(tuples, credit=2), whole)

    def test_frozen_frontier_preview_is_not_sequential_preview(self):
        tuples = (TupleCost("a", 10), TupleCost("b", 20))
        frozen_preview = tuple(item.event for item in tuples if item.cost < 100)
        actual = per_tuple(tuples, credit=2)
        self.assertEqual(frozen_preview, ("a", "b"))
        self.assertEqual(actual.published, ("a",))
        self.assertEqual(actual.rejected, ("b",))


if __name__ == "__main__":
    case = (TupleCost("a", 10), TupleCost("b", 5))
    print(json.dumps({
        "scope": "independent finite contract model; not Rust or performance evidence",
        "baseline_already_complete": True,
        "remaining_optional_credit": 1,
        "per_tuple": asdict(per_tuple(case, 1)),
        "whole_expression_atomic": asdict(atomic_expression(case, 1)),
    }, indent=2), flush=True)
    unittest.main()
