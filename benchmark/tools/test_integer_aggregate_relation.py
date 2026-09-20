"""Relation/ordering negative controls for the registered numerical oracle."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_result_difference import encode_value
from verify_integer_aggregate_relation import verify, VERSION, Uncovered


def capture(rows):
    encoded = [[encode_value(value) for value in row] for row in rows]
    return {"actual": encoded, "expected": copy.deepcopy(encoded),
            "oracle_status": "exact_match"}


class RelationOracleTests(unittest.TestCase):
    def setUp(self):
        self.spec = {"contract": VERSION, "input_keys": [0, 1], "input_value": 2,
                     "join_keys": [0], "selector_key": 1, "left_value": 1,
                     "right_value": 2, "threshold": 1, "output_width": 8,
                     "unique_order_prefix": [0, 1], "roles": [
                         {"keys": [0, 1], "mean": 2, "cv": 3},
                         {"keys": [4, 5], "mean": 6, "cv": 7}]}
        self.inputs = capture([(key, selector, value)
                               for key in [7, 8] for selector in [1, 2]
                               for value in [0, 0, 3, None]])
        self.rows = [(key, 1, 1.0, 3**0.5, key, 2, 1.0, 3**0.5)
                     for key in [7, 8]]

    def test_complete_relation_and_empty_input(self):
        result = verify(self.inputs, capture(self.rows), self.spec)
        self.assertEqual(result["reference_rows"], 2)
        self.assertEqual(verify(capture([]), capture([]), self.spec)["reference_rows"], 0)

    def test_lost_row_duplicate_and_wrong_order_fail(self):
        for rows in [self.rows[:1], [self.rows[0]]*2, list(reversed(self.rows))]:
            with self.assertRaises((ValueError, AssertionError)):
                verify(self.inputs, capture(rows), self.spec)

    def test_threshold_and_approximate_limit_are_not_output_tolerance(self):
        for selector in [1, 2]:
            rows = [(7, selector, v) for v in [14, 132, 325]]
            self.assertEqual(verify(capture(rows), capture([]), self.spec)
                             ["exact_threshold_groups_excluded"], 1)
        with self.assertRaises(Uncovered):
            verify(self.inputs, capture(self.rows), dict(self.spec, limit=1))

    def test_unregistered_version_and_extra_columns_fail(self):
        with self.assertRaises(ValueError):
            verify(self.inputs, capture(self.rows), dict(self.spec, contract="unknown"))
        with self.assertRaises(ValueError):
            verify(capture([(7, 1, 3, 4)]), capture([]), self.spec)
