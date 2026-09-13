"""E3 read-only report analysis; cohorts are never pooled across instruments."""
import gzip
import json
import random
import statistics as s
import sys
from pathlib import Path


def read(path):
    with (gzip.open(path, 'rt') if str(path).endswith('.gz') else open(path)) as f:
        return json.load(f)


def interval(ratios):
    rng = random.Random(0)
    samples = sorted(s.geometric_mean(rng.choices(ratios, k=len(ratios)))
                     for _ in range(1000))
    return {'geometric_ratio': s.geometric_mean(ratios),
            'bootstrap_95': [samples[24], samples[974]],
            'one_sided_95_upper': samples[949], 'pairs': len(ratios)}


groups = {}
identities = []
for path in sys.argv[1:]:
    report = read(path)
    assert report['source']['dirty'] is False
    q = report['queries'][0]
    assert q['status'] == 'passed' and q['rows'] == 90
    assert q['cold_statement']['trace_off_verified']
    env = report['environment']['runtime_environment'] if 'environment' in report else None
    if env is None:
        env = next(value['runtime_environment'] for value in report.values()
                   if isinstance(value, dict) and 'runtime_environment' in value)
    arm = 'stream' if env['PARO_DIAGNOSTIC_STREAM_SEQUENTIAL'] == '1' else 'control'
    cohort = 'touch' if report['pre_touch'] else ('scalar' if env['PARO_COLD_WORK_EVIDENCE'] == '1' else 'normal')
    key = cohort + '-' + arm
    identities.append({'report': path, 'source': report['source'],
                       'binary_sha256': report['build_attestation']['binary_sha256'],
                       'sql_sha256': report['query_corpus_sha256']})
    for b in q['process_blocks']:
        e = b['cold_miss_evidence']
        assert e['status'] == 'verified' and e['cache_hit'] is False and e['occurrence'] == 0
        row = {'report': path, 'block': b['block'],
               'target_ms': b['cold_statement_ms']['paro'],
               'duckdb_target_ms': b['cold_statement_ms']['duckdb'],
               'warm_ms': s.median(b['paro_ms']), 'warm_samples_ms': b['paro_ms'],
               'duckdb_warm_ms': s.median(b['duckdb_ms']),
               'compiler_ms': e['compile_work']['compiler_elapsed_us']/1000,
               'synthesis': e['compile_work']['child_combination_cost_synthesis_count']}
        if cohort == 'scalar':
            m = e['execution_work']['metrics']
            row['execution_ms'] = m['execution_elapsed_us']/1000
            for name in ['buffer_fill_count', 'buffer_fill_input_bytes', 'decoder_count',
                         'decoder_input_bytes', 'minor_faults']:
                row[name] = m[name]
            row['image_id'] = m['image_id']
            row['warm_execution'] = b['paro_execution_work']
        if cohort == 'touch':
            row['pre_touch'] = b['pre_touch']
        groups.setdefault(key, []).append(row)

assert len({i['binary_sha256'] for i in identities}) == 1
assert len({i['sql_sha256'] for i in identities}) == 1
summary = {}
for key, rows in groups.items():
    summary[key] = {name: s.median([r[name] for r in rows]) for name in rows[0]
                    if isinstance(rows[0][name], (int, float)) and name not in ('block', 'image_id')}
    summary[key]['paired_paro_duckdb'] = interval([r['target_ms']/r['duckdb_target_ms'] for r in rows])
    targets = sorted(r['target_ms'] for r in rows)
    summary[key]['observed_p95_ms'] = targets[round((len(targets)-1)*.95)]
if 'normal-control' in groups and 'normal-stream' in groups:
    assert len(groups['normal-control']) == len(groups['normal-stream'])
    summary['mechanism_AB'] = interval([p['target_ms']/c['target_ms'] for c, p in
                                       zip(groups['normal-control'], groups['normal-stream'])])
print(json.dumps({'identities': identities, 'blocks': groups, 'summary': summary,
                  'warning': 'Pilot; scalar/pre-touch timing not normal C1, no formal W noninferiority or tail claim.'}, indent=2))
