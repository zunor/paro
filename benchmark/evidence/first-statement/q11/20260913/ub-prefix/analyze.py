"""Fixed pilot: paired prefix-preparation assay, not a new two-N U fit."""
import json
import math
from pathlib import Path
import random
import re
import statistics as s

root = Path('/private/tmp')
arms = {}
for name in ('c0', 'p0', 'p1', 'c1'):
    report = json.loads((root / f'paro-ub-prefix-{name}.json').read_text())
    assert not report['source']['dirty']
    query = report['queries'][0]
    assert query['status'] == 'passed' and query['rows'] == 90
    rows = []
    for block in query['process_blocks']:
        evidence = block['cold_miss_evidence']
        assert evidence['status'] == 'verified' and evidence['occurrence'] == 0 and evidence['cache_hit'] is False
        assert block['statement_trace']['verified_empty']
        work = evidence['compile_work']
        assert work['child_combination_cost_synthesis_count'] == 1691
        rows.append({'block': block['block'], 'C1': block['cold_statement_ms']['paro'],
                     'DC1': block['cold_statement_ms']['duckdb'], 'W': s.median(block['paro_ms']),
                     'DW': s.median(block['duckdb_ms']), 'compiler': work['compiler_elapsed_us']/1000,
                     'optimizer': work['optimizer_elapsed_us']/1000,
                     'rules': work['rule_elapsed_us']/1000,
                     'R': (work['optimizer_elapsed_us']-work['rule_elapsed_us'])/1000})
    assert len(rows) == 2
    log = (root / f'paro-ub-prefix-{name}.q11.diagnostic000.parod.log').read_text()
    log = re.sub(r'\x1b\[[0-9;]*m', '', log)
    choices = [line for line in log.splitlines() if 'diagnostic portfolio admission choice' in line]
    identities = sorted(set(re.search(r'physical_fingerprint=([0-9a-f]+)', line).group(1) for line in choices))
    trace = query['diagnostic_cohort']['process_blocks'][0]['target_statement_traces'][0]
    winner_fields = {event['event']: event['value'] for event in trace['events']
                     if event['event'].startswith('final_winner_')
                     and 'fingerprint_' in event['event']}
    arms[name] = {'source': report['source'], 'binary': report['build_attestation']['binary_sha256'],
                  'rows': rows, 'diagnostic_admission_fingerprints': identities,
                  'final_winner_fingerprints': winner_fields}
assert arms['c0']['final_winner_fingerprints']
for arm in arms.values():
    assert arm['final_winner_fingerprints'] == arms['c0']['final_winner_fingerprints']
    assert arm['diagnostic_admission_fingerprints'] == ['5c29cf646706c8c8ba84000150211a6b']
control = arms['c0']['rows'] + arms['c1']['rows']
probe = arms['p0']['rows'] + arms['p1']['rows']
delta = [c['R']-p['R'] for c,p in zip(control,probe)]
ratios = [math.log(p['C1']/c['C1']) for c,p in zip(control,probe)]
rng = random.Random(1)
boot = sorted(math.exp(s.mean(rng.choices(ratios,k=4))) for _ in range(10000))
result = {'arms': arms, 'summary': {
    name: {key:s.median(row[key] for row in rows) for key in rows[0] if key!='block'}
    for name,rows in [('control',control),('probe',probe)]},
    'paired_C1_ratio': math.exp(s.mean(ratios)), 'paired_C1_bootstrap95': [boot[249],boot[9749]],
    'paired_residual_saved_ms': delta, 'mean_residual_saved_ms':s.mean(delta),
    'conditional_U_proxy_us':20.4139-s.mean(delta)*1000/1691,
    'warning':'N4 pilot, inherited asymmetric metadata, no formal NI/parity; conditional U proxy assumes unchanged intercept, not measured synthesis function time or whole-protocol assay.'}
print(json.dumps(result,indent=2))
