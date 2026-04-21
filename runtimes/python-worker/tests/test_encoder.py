import unittest
import warnings

from paro_runtime_worker.column_decoder import ColumnDecoder
from paro_runtime_worker.column_encoder import ColumnEncoder
from paro_udf.column import Column, SlowPathWarning


class EncoderTests(unittest.TestCase):
    def test_encoder_emits_expected_descriptor_shapes(self) -> None:
        encoder = ColumnEncoder()
        encoded = encoder.encode_result(
            (
                Column.from_constant(7, logical_type="int64", length=3, name="constant_score"),
                Column(["x", "y", "z"], logical_type="varchar", name="label"),
            ),
            row_count=3,
            logical_types=("int64", "varchar"),
            output_names=("constant_score", "label"),
        )

        self.assertEqual(encoded.row_count, 3)
        self.assertEqual(encoded.columns[0]["name"], "constant_score")
        self.assertIn("Constant", encoded.columns[0]["layout"])
        self.assertIn("VarLen", encoded.columns[1]["layout"])
        self.assertGreaterEqual(len(encoded.buffers), 2)

    def test_encoder_roundtrips_through_decoder(self) -> None:
        columns = [
            Column.from_sequence(10, 5, "int64", length=3, name="seq"),
            Column.from_dictionary([0, 1, 0], ["a", "b"], logical_type="varchar", name="dict"),
        ]
        encoded = ColumnEncoder().encode_columns(columns)
        decoded = ColumnDecoder().decode_batch(
            encoded.to_payload(lease_id=4),
            encoded.buffers,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SlowPathWarning)
            self.assertEqual(decoded.columns[0].materialize_py(), [10, 15, 20])
            self.assertEqual(decoded.columns[1].materialize_py(), ["a", "b", "a"])


if __name__ == "__main__":
    unittest.main()
