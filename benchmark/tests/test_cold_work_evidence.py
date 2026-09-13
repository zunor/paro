import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'corpora'))
from tpcds_compare import execution_work_from_rows

class ColdWorkEvidence(unittest.TestCase):
    def test_execution_identity_not_cache_lookup_or_other_query(self):
        columns = {'name':0, 'kind':1, 'metric_value':2}
        rows = [('statement_execution_work/000000000000000b/2/minor_faults','evidence',9),
                ('statement_execution_work/000000000000000b/7/minor_faults','evidence',3),
                ('statement_execution_work/0000000000000016/8/minor_faults','evidence',99)]
        record = execution_work_from_rows(rows, columns, 11)
        self.assertEqual(record['execution_id'], 7)
        self.assertEqual(record['metrics'], {'minor_faults':3})
        self.assertEqual(execution_work_from_rows(rows, columns, 33), {})

if __name__ == '__main__':
    unittest.main()
