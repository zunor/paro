"""Recompute E2 block-level differences; pre-touch target is NOT normal C1."""
import gzip
import json
import random
import statistics as s
import sys
from pathlib import Path


def read(path):
    path = Path(path)
    with (gzip.open(path, 'rt') if path.suffix == '.gz' else path.open()) as stream:
        return json.load(stream)


groups = {'control': [], 'touch': []}
identities = []
for path in sys.argv[1:]:
    report = read(path)
    assert not report['source']['dirty']
    q = report['queries'][0]
    assert q['status'] == 'passed' and q['rows'] == 90
    assert q['cold_statement']['trace_off_verified']
    arm = 'touch' if report['pre_touch'] else 'control'
    identities.append({'report':path, 'source':report['source'],
                       'binary_sha256':report['build_attestation']['binary_sha256'],
                       'query_corpus_sha256':report['query_corpus_sha256']})
    for block in q['process_blocks']:
        evidence = block['cold_miss_evidence']
        assert evidence['status'] == 'verified' and evidence['cache_hit'] is False
        assert evidence['occurrence'] == 0
        compiler = evidence['compile_work']['compiler_elapsed_us']/1000
        target = block['cold_statement_ms']['paro']
        warm = s.median(block['paro_ms'])
        item = {'report':path, 'block':block['block'], 'target_ms':target,
                'compiler_ms':compiler, 'warm_ms':warm,
                'residual_ms_not_exclusive_phase':target-compiler-warm,
                'duckdb_target_ms':block['cold_statement_ms']['duckdb'],
                'synthesis':evidence['compile_work']['child_combination_cost_synthesis_count']}
        if arm == 'touch':
            prep = block['pre_touch']
            item['pre_touch_paro_ms'] = prep['engines']['paro']['execute_fetch_ms']
            item['pre_touch_duckdb_ms'] = prep['engines']['duckdb']['execute_fetch_ms']
            item['preparation_both_engines_wall_ms'] = prep['preparation_wall_ms']
            item['paro_pre_touch_plus_target_ms'] = target+item['pre_touch_paro_ms']
        groups[arm].append(item)

assert len({i['binary_sha256'] for i in identities}) == 1
assert len({i['query_corpus_sha256'] for i in identities}) == 1
assert len(groups['touch']) == len(groups['control'])
ratios = [t['target_ms']/c['target_ms'] for c,t in zip(groups['control'],groups['touch'])]
differences = [c['target_ms']-t['target_ms'] for c,t in zip(groups['control'],groups['touch'])]
rng = random.Random(0)
boot = sorted(s.geometric_mean(rng.choices(ratios,k=len(ratios))) for _ in range(1000))
summary = {arm:{key:s.median([item[key] for item in items]) for key in items[0]
                if key not in ['report','block']} for arm,items in groups.items()}
print(json.dumps({'identities':identities,'blocks':groups,'median_of_block_metrics':summary,
    'adjacent_fresh_pair_target_ratios':ratios, 'adjacent_fresh_pair_saved_ms':differences,
    'target_ratio':s.geometric_mean(ratios), 'bootstrap_samples':1000,
    'bootstrap_95': [boot[24],boot[974]],
    'warning':'Four adjacent fresh-process pairs; pre-touch timing is diagnostic, not production C1/parity. Preparation excluded only from target, explicitly reported. No phase inference by subtracting campaign medians.'},indent=2))
