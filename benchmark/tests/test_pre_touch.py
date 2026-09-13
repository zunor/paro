from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'corpora'))
from benchmark_evidence import statement_fingerprint
from tpcds_compare import read_pre_touch, require_first_target_miss, collect_pre_touch


class PreTouchTests(unittest.TestCase):
    def test_single_select_only(self):
        for sql in ['', 'SELECT 1; SELECT 2', 'CREATE TABLE x(a INT)']:
            with patch.object(Path, 'read_text', return_value=sql):
                with self.assertRaises(ValueError):
                    read_pre_touch(Path('unused.sql'))
        with patch.object(Path, 'read_text', return_value='SELECT 1'), \
                patch('tpcds_compare.content_digest', return_value='digest'):
            spec = read_pre_touch(Path('pre.sql'))
            self.assertEqual(spec['sha256'], 'digest')
            self.assertEqual(spec['query_fingerprint'], statement_fingerprint('SELECT 1'))
        self.assertIsNone(read_pre_touch(None))

    def test_target_cache_gate_cannot_be_relaxed(self):
        good = dict(status='verified', occurrence=0, cache_hit=False,
                    query_fingerprint=statement_fingerprint('SELECT 2'))
        require_first_target_miss(good, 'SELECT 2')
        for change in [dict(cache_hit=True), dict(status='uncovered'),
                       dict(occurrence=1), dict(query_fingerprint=0)]:
            with self.assertRaises(AssertionError):
                require_first_target_miss(good | change, 'SELECT 2')

    def test_no_touch_and_target_collision_do_not_execute(self):
        with patch('tpcds_compare.timed_run_paro') as execute:
            self.assertIsNone(collect_pre_touch(None, None, None, 'SELECT 2', True))
            with self.assertRaises(ValueError):
                collect_pre_touch(None, None, {'query_fingerprint':statement_fingerprint('SELECT 2')}, 'SELECT 2', True)
            execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
