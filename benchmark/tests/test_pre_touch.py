from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'corpora'))
from benchmark_evidence import statement_fingerprint
from tpcds_compare import (read_pre_touch, require_first_target_miss, collect_pre_touch,
                           collect_statement_cache_evidence)


class PreTouchTests(unittest.TestCase):
    def test_explicit_second_occurrence_does_not_relax_first_miss_gate(self):
        fp = statement_fingerprint('SELECT 1')
        identity = {"schema_version": 3, "artifact": [1, 2],
                    "structure": [3, 4], "dependencies": [5, 6]}
        cursor = Mock()
        cursor.description = []
        for name in ('name', 'kind', 'last_elapsed_us', 'metric_value', 'metric_unit',
                     'invocation_count', 'record_type', 'record_id', 'payload_json'):
            col = Mock()
            col.name = name
            cursor.description.append(col)
        def row(record_type, record_id, payload):
            return (record_type, 'receipt', 0, 0, 'receipt', 1,
                    record_type, record_id, json.dumps(payload))
        compile_receipt = {
            'schema_version': 3, 'artifact_identity': identity,
            'search_stop': {'Observed': 'QualityPolicySatisfied'},
            'search_complete': {'Observed': False},
            'quality_policy_satisfied': {'Observed': True},
            'budget_limited': {'Observed': False},
            'obligations': {'Observed': 0}, 'groups': {'Observed': 1},
            'logical_expressions': {'Observed': 1},
            'physical_expressions': {'Observed': 1},
            'expected_class': {'Observed': 2}, 'variant_count': {'Observed': 1},
            'omitted_variants': 0, 'compile_work': None,
        }
        cursor.fetchall.return_value = [
            row('statement_cache', 6, {
                'schema_version': 3, 'decision_id': 6, 'query_fingerprint': fp,
                'occurrence': 9, 'cache_hit': True, 'artifact_identity': identity,
                'compile_work': None, 'compile_receipt': compile_receipt}),
            row('execution_receipt', 7, {
            'schema_version': 3, 'execution_id': 7, 'statement_decision_id': 6,
                'artifact_identity': identity, 'expected_class': 2,
                'actual_class': 2, 'actual_fingerprint': [7, 8],
                'resources': {
                    'class': 2, 'minimum_memory_bytes': 100,
                    'working_set_memory_bytes': 200, 'memory_ceiling_bytes': 1000,
                    'memory_completion': 'Guaranteed', 'max_parallel_tasks': 4,
                    'external_worker_slots': 0,
                },
                'admission': 'Selected', 'fallback': None,
                'reservation': 'Committed', 'lowering': 'Ready',
                'lowering_error': None, 'image': 'Ready', 'terminal': 'Completed',
                'terminal_error': None}),
        ]
        connection = Mock()
        connection.cursor.return_value.__enter__ = Mock(return_value=cursor)
        connection.cursor.return_value.__exit__ = Mock(return_value=False)
        self.assertEqual(collect_statement_cache_evidence(connection, 'SELECT 1')['status'], 'Uncovered')
        evidence = collect_statement_cache_evidence(
            connection, 'SELECT 1', before_execution_ids=set())
        self.assertEqual(evidence['compile']['occurrence'], 9)
        self.assertEqual(evidence['compilation'], 'CacheHit')
        with self.assertRaises(AssertionError):
            require_first_target_miss(evidence, 'SELECT 1')

    def test_second_pre_touch_is_separate_and_validated(self):
        spec = dict(sql='SELECT 1', query_fingerprint=statement_fingerprint('SELECT 1'),
                    repetitions=2)
        duck = Mock()
        duck.execute.side_effect = [([(1,)], [], 20), ([(1,)], [], 12)]
        with patch('tpcds_compare.timed_run_paro', side_effect=[
                ([(1,)], [], 132), ([(1,)], [], 40)]) as run, \
             patch('tpcds_compare.canonicalize_rows', side_effect=lambda rows, _: rows), \
             patch('tpcds_compare.multiset_digest', return_value='same'), \
             patch('tpcds_compare.schema_report', return_value=[]), \
             patch('tpcds_compare.assert_compatible_schema'), \
             patch('tpcds_compare.assert_same_multiset') as compare, \
             patch('tpcds_compare.collect_statement_cache_evidence', return_value={}):
            record = collect_pre_touch(None, duck, spec, 'SELECT 2', True)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(compare.call_count, 3)
        self.assertEqual(record['engines']['paro']['execute_fetch_ms'], 132)
        self.assertEqual(record['engines']['paro']['second_execution']['execute_fetch_ms'], 40)
        self.assertEqual(record['engines']['duckdb']['second_execution']['execute_fetch_ms'], 12)
        self.assertFalse(record['target_is_normal_c1'])
        for repetitions in (0, 3, 2):
            with self.assertRaises(ValueError):
                read_pre_touch(None, repetitions)

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
        good = dict(status='Verified', occurrence=9, compilation='Executed',
                    compile={'cache_hit': False},
                    query_fingerprint=f"{statement_fingerprint('SELECT 2'):016x}")
        require_first_target_miss(good, 'SELECT 2')
        for change in [dict(compilation='CacheHit'), dict(status='uncovered'),
                       dict(status='Uncovered'), dict(query_fingerprint=0),
                       dict(compile={}), dict(compile={'cache_hit': True})]:
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
