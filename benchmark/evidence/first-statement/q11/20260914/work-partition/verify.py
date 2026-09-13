#!/usr/bin/env python3
"""Validate identities/work, preserving the explicitly bounded replay scope."""
import gzip
import hashlib
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parent
raw = root / 'raw'
for item in json.loads((raw / 'manifest.json').read_text()):
    data = (raw / item['file']).read_bytes()
    assert hashlib.sha256(data).hexdigest() == item['gzip_sha256']
    assert hashlib.sha256(gzip.decompress(data)).hexdigest() == item['raw_sha256']

def contract(arm):
    d = json.loads(gzip.decompress((raw / f'paro-partition-{arm}.json.gz').read_bytes()))
    assert not d['source']['dirty'] and d['failed'] == 0
    events = d['queries'][0]['diagnostic_cohort']['process_blocks'][0]['target_statement_traces'][0]['events']
    values = {e['event']: e['value'] for e in events}
    rows = {}
    for e in events:
        match = re.fullmatch(r'candidate_lifecycle_(\d+)\.(.+)', e['event'])
        if match and match[2] != 'elapsed_us':
            rows.setdefault(int(match[1]), {})[match[2]] = e['value']
    counts = {k: values[k] for k in (
        'memo_group_count', 'memo_logical_expression_count', 'memo_physical_expression_count',
        'child_combination_cost_synthesis_count', 'published_winner_count',
        'transformation_binding_count', 'physical_implementation_request_count',
        'physical_subproblem_request_count', 'child_combination_recompute_count',
        'task_registry_request_count', 'task_registry_reuse_count',
        'task_registry_unique_evaluation_count', 'quality_policy_candidate_evaluation_count')}
    log = gzip.decompress((raw / f'paro-partition-{arm}.q11.diagnostic000.parod.log.gz').read_bytes()).decode()
    log = re.sub(r'\x1b\[[0-9;]*m', '', log)
    admitted = re.findall(r'diagnostic portfolio admission choice.*physical_fingerprint=([a-f0-9]+)', log)
    assert admitted and set(admitted) == {'5c29cf646706c8c8ba84000150211a6b'}
    return dict(counts=counts, admitted=sorted(set(admitted)),
                publications={k:v for k,v in values.items() if k.startswith('rule.') and k.endswith('.published')},
                final_fingerprints={k:v for k,v in values.items() if k.startswith('final_winner_') and 'fingerprint' in k},
                captured=[r for r in rows.values() if r['stage'] in (2,3,4)],
                stored=values['candidate_lifecycle_stage_3.stored'],
                dropped=values['candidate_lifecycle_stage_3.dropped'])

arms = ('v2-off0','v2-on0','v2-off1','probe-off0','probe-on0','probe-off1')
base = contract(arms[0])
for arm in arms[1:]:
    assert contract(arm) == base, arm
result = dict(arms=list(arms), counters=base['counts'], admitted=base['admitted'],
              logical_outputs=sum(base['publications'].values()),
              final_fingerprint_fields=len(base['final_fingerprints']),
              matched_captured_events=len(base['captured']),
              tuples_stored=base['stored'], tuples_dropped=base['dropped'],
              full_prefix_replay=False)
(root / 'checks.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
