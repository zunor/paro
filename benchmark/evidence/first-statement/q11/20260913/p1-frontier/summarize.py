#!/usr/bin/env python3
"""Recompute exact-SQL pilot scalars and explicit bounded replay coverage."""
import gzip
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load(arm):
    return json.load(gzip.open(ROOT / 'raw' / f'paro-p1-{arm}-exact.json.gz'))


def events(report):
    return report['queries'][0]['diagnostic_cohort']['process_blocks'][0]['target_statement_traces'][0]['events']


def replay(report):
    es = events(report)
    rows = {}
    for e in es:
        m = re.fullmatch(r'candidate_lifecycle_(\d+)\.(.+)', e['event'])
        if m and m[2] != 'elapsed_us':
            rows.setdefault(int(m[1]), {})[m[2]] = e['value']
    return dict(
        publications={e['event']: e['value'] for e in es if e['event'].startswith('rule.') and e['event'].endswith('.published')},
        fingerprints={e['event']: e['value'] for e in es if e['event'].startswith('final_winner_') and 'fingerprint' in e['event']},
        captured_candidates=[v for v in rows.values() if v['stage'] in (2, 3, 4)],
    )


def summarize(arm):
    d = load(arm)
    assert not d['source']['dirty'] and d['failed'] == 0
    assert d['query_corpus_sha256'] == 'a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503'
    q = d['queries'][0]
    assert q['rows'] == 90
    for b in q['process_blocks']:
        miss = b['cold_miss_evidence']
        assert miss['status'] == 'verified' and miss['occurrence'] == 0 and not miss['cache_hit']
    works = [b['cold_miss_evidence']['compile_work'] for b in q['process_blocks']]
    es = events(d)
    values = {e['event']: e['value'] for e in es}
    spans = {e['event']: e['duration_us'] for e in es if e['duration_us'] is not None}
    return dict(
        source=d['source']['commit'], binary=d['build_attestation']['binary_sha256'],
        source_query_corpus=d['query_corpus_sha256'],
        instrumented=arm.startswith(('on', 'w')) or arm == 'phaseon0',
        additional_environment={
            'PARO_DIAGNOSTIC_FRONTIER_WIDTH': {'w1': '1', 'w2': '2', 'w4': '4', 'w8': '8', 'winf': 'unbounded'}.get(arm),
            'PARO_DIAGNOSTIC_FRONTIER_SNAPSHOT': f'/private/tmp/paro-p1-{arm}-exact.snapshot.jsonl' if arm.startswith(('on', 'w')) else None,
            'PARO_DIAGNOSTIC_COST_PHASE_TIMES': '1' if arm == 'phaseon0' else None,
        },
        block_scalars=works,
        median_scalars={k: statistics.median(w[k] for w in works) for k in works[0]},
        c1=q['cold_statement'], warm=q['paro'], warm_ratio=q['warm_paro_over_duckdb'],
        diagnostic=dict(optimizer_us=spans.get('optimizer'),
                        rules_us=sum(e['value'] for e in es if e['event'].startswith('rule.') and e['event'].endswith('.elapsed_us')),
                        kernel_ns=values.get('diagnostic_cost_kernel_ns'),
                        admission_ns=values.get('diagnostic_candidate_admission_ns'),
                        tuple_stored=values.get('candidate_lifecycle_stage_3.stored'),
                        tuple_dropped=values.get('candidate_lifecycle_stage_3.dropped'),
                        complete=values.get('governor_search_complete'),
                        ready_us=values.get('quality_policy_satisfied_us')),
    )


if __name__ == '__main__':
    arms = ('off0', 'on0', 'off1', 'w1', 'w2', 'w4', 'w8', 'winf',
            'p2probe', 'p2control', 'phaseoff0', 'phaseon0', 'phaseoff1')
    results = {a: summarize(a) for a in arms}
    for before, after in [('p2control', 'p2probe'), ('phaseoff0', 'phaseon0'), ('phaseoff1', 'phaseon0')]:
        assert replay(load(before)) == replay(load(after)), (before, after)
    results['replay_scope'] = 'All captured ChildReady/TuplePriced/ParentPublished payloads and final fingerprint fields match; only128/1691 tuples captured, NOT full admitted-prefix replay.'
    (ROOT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
