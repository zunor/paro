import unittest
import warnings

from paro_runtime_worker.column_decoder import ColumnDecoder
from paro_runtime_worker.column_encoder import ColumnEncoder
from paro_udf.column import Column, SlowPathWarning


class DecoderTests(unittest.TestCase):
    def test_decoder_roundtrips_fixed_varlen_and_list_columns(self) -> None:
        columns = [
            Column([1, 2, 3], logical_type="int64", name="numbers"),
            Column(
                ["a", "bb", None],
                logical_type="varchar",
                null_mask=[False, False, True],
                name="labels",
            ),
            Column([[1, 2], [3], []], logical_type="list<int64>", encoding="list", name="groups"),
        ]
        encoded = ColumnEncoder().encode_columns(columns)
        lease = encoded.to_payload(
            lease_id=42,
            ownership={
                "owner_worker_epoch": 9,
                "owner_host_epoch": 7,
                "owner_query_epoch": 3,
            },
        )

        decoded = ColumnDecoder().decode_batch(
            lease,
            encoded.buffers,
            expected_host_epoch=7,
            expected_query_epoch=3,
        )
        self.assertEqual(decoded.row_count, 3)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SlowPathWarning)
            self.assertEqual(decoded.columns[0].materialize_py(), [1, 2, 3])
            self.assertEqual(decoded.columns[1].materialize_py(), ["a", "bb", None])
            self.assertEqual(decoded.columns[2].materialize_py(), [[1, 2], [3], []])

    def test_decoder_rejects_epoch_mismatch(self) -> None:
        column = Column([1, 2], logical_type="int64", name="numbers")
        encoded = ColumnEncoder().encode_columns([column])
        lease = encoded.to_payload(
            lease_id=9,
            ownership={
                "owner_worker_epoch": 5,
                "owner_host_epoch": 11,
                "owner_query_epoch": 13,
            },
        )

        with self.assertRaisesRegex(Exception, "host epoch mismatch"):
            ColumnDecoder().decode_batch(lease, encoded.buffers, expected_host_epoch=7)


if __name__ == "__main__":
    unittest.main()
