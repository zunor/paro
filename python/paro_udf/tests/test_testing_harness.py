import unittest
import warnings

from paro_udf.column import SlowPathWarning
from paro_udf.testing.harness import assert_column_equal, make_column, make_context


class TestingHarnessTests(unittest.TestCase):
    def test_testing_harness_constructs_context_and_encoded_columns(self) -> None:
        ctx = make_context(
            7,
            query_id=3,
            routine_identity="routine@9",
            capability_profile="compiled_kernel",
            test_case="sdk",
        )
        left = make_column((10, 2), encoding="sequence", length=3, logical_type="int64")
        right = make_column((10, 2), encoding="sequence", length=3, logical_type="int64")

        self.assertEqual(ctx.batch_id, 7)
        self.assertEqual(ctx.query_id, 3)
        self.assertEqual(ctx.metadata["test_case"], "sdk")
        assert_column_equal(left, right)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            self.assertEqual(left.materialize_py(), [10, 12, 14])
        self.assertTrue(any(item.category is SlowPathWarning for item in recorded))

    def test_dictionary_columns_can_be_compared(self) -> None:
        left = make_column(
            {"indices": [0, 1, 0], "dictionary": ["a", "b"]},
            logical_type="varchar",
            encoding="dictionary",
        )
        right = make_column(
            {"indices": [0, 1, 0], "dictionary": ["a", "b"]},
            logical_type="varchar",
            encoding="dictionary",
        )
        assert_column_equal(left, right)


if __name__ == "__main__":
    unittest.main()
