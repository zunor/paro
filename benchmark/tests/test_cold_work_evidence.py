import sys
import unittest
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'corpora'))
from tpcds_compare import execution_work_from_rows

class ColdWorkEvidence(unittest.TestCase):
    def test_execution_identity_not_cache_lookup_or_other_query(self):
        columns = {'record_type': 0, 'record_id': 1, 'payload_json': 2}
        rows = [
            ('execution_work', 2, json.dumps({
                'schema_version': 1, 'execution_id': 2,
                'query_fingerprint': 11, 'metrics': {'minor_faults': 9}})),
            ('execution_work', 7, json.dumps({
                'schema_version': 1, 'execution_id': 7,
                'query_fingerprint': 11, 'metrics': {'minor_faults': 3}})),
            ('execution_work', 8, json.dumps({
                'schema_version': 1, 'execution_id': 8,
                'query_fingerprint': 22, 'metrics': {'minor_faults': 99}})),
        ]
        record = execution_work_from_rows(rows, columns, 11, before_execution_ids={2})
        self.assertEqual(record['execution_id'], 7)
        self.assertEqual(record['metrics'], {'minor_faults':3})
        self.assertEqual(execution_work_from_rows(rows, columns, 33, before_execution_ids={2}), {})

if __name__ == '__main__':
    unittest.main()
