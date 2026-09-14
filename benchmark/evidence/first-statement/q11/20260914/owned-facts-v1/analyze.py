#!/usr/bin/env python3
"""Recompute exact identities and exclusive buckets. No cross-cohort subtraction."""
import gzip
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW = ROOT / 'raw'
ANCHOR = {'admitted_fingerprint': '5c29cf646706c8c8ba84000150211a6b',
          'syntheses': 1691, 'published': 891}


def read(name):
    path = RAW / name
    return gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()


def diagnostic(name):
    d = json.loads(read(f'paro-owned-facts-{name}.json.gz'))
    assert not d['source']['dirty'] and d['rows'] == 90
    assert d['miss']['status'] == 'verified' and d['miss']['occurrence'] == 0 and not d['miss']['cache_hit']
    trace = next(t for t in d['traces'] if t['query_fingerprint'] == d['query_fingerprint'])
    values = d['values']
    # Wall-clock checkpoints are intentionally separate: reaching a different
    # point at 20ms is not a changed fixed-work search prefix.
    fixed = {k: v for k, v in values.items() if k.startswith('final_winner_2.')
             or ('.' not in k and k.endswith('_count') and 'lifecycle' not in k)
             or (k.startswith('rule.') and not any(s in k for s in ('_us', '_ns', 'allocated')))}
    return d, dict(source=d['source']['commit'], triple=d['triple'], anchor=ANCHOR,
        optimizer_us=next(e['duration_us'] for e in trace['events'] if e['event'] == 'optimizer'),
        quality_us=values.get('quality_policy_satisfied_us'),
        search_complete=values.get('governor_search_complete'), fixed=fixed)


def ledger(name):
    d, result = diagnostic(name)
    rows = [json.loads(line) for line in read(f'paro-owned-facts-{name}.ledger.jsonl.gz').splitlines()]
    # This helper issues the target SELECT before all diagnostic SQL. The
    # first optimizer ledger belongs to that statement in this exact process.
    row = rows[0]
    assert row['pid'] == d['process']['pid'] and row['success']
    assert row['statement'].lstrip().lower().startswith('with year_total')
    assert abs(row['total_ns'] / 1000 - result['optimizer_us']) < 10
    assert row['total_ns'] == row['sum_ns'] == sum(b['exclusive_ns'] for b in row['buckets'])
    b3 = [b for b in row['buckets'] if b['bucket'].startswith('B3')]
    b3_ns = sum(b['exclusive_ns'] for b in b3)
    per_rule_ns = sum(sum(r['ns']) for r in row['b3_by_rule'].values())
    assert b3_ns == per_rule_ns
    result = {'total_ns': row['total_ns'], 'b3_total_ns': b3_ns,
              'b3_coverage': 1 - b3[0]['exclusive_ns'] / b3_ns,
              'optimizer_coverage': 1 - row['buckets'][-1]['exclusive_ns'] / row['total_ns'],
              'b3_by_rule': row['b3_by_rule'], 'buckets': row['buckets']}
    for b in result['buckets']:
        b['us_per_interval'] = b['exclusive_ns'] / 1000 / b['entries'] if b['entries'] else None
    return result


def normal(name):
    d = json.loads(read(f'paro-owned-facts-{name}.json.gz'))
    assert not d['source']['dirty'] and d['failed'] == 0
    q = d['queries'][0]
    assert q['rows'] == 90 and q['cold_statement']['trace_off_verified']
    blocks = []
    for b in q['process_blocks']:
        miss = b['cold_miss_evidence']
        assert miss['status'] == 'verified' and miss['occurrence'] == 0 and not miss['cache_hit']
        blocks.append(dict(block=b['block'], c1=b['cold_statement_ms'],
                           paro_w=b['paro_ms'], duckdb_w=b['duckdb_ms'], work=miss['compile_work']))
    return dict(source=d['source'], cold=q['cold_statement'], warm=q['warmup_and_steady_state'], blocks=blocks)


def main():
    summary = {'anchor': ANCHOR, 'diagnostics': {}, 'normal': {}, 'partitions': {}}
    for name in ['t0-3415b2de-diag', 't0-187140e2-diag', 't0-9182bea0-diag',
                 't0-9dbfd547-diag', 't0-f5adaed4-diag', 't0-f1ccd481-diag',
                 't0-b3aa3365-diag', 't0-deferral-fixed-diag',
                 't1-off0', 't1-on0', 't1-off1', 't2-off0', 't2-on0', 't2-off1']:
        _, item = diagnostic(name)
        summary['diagnostics'][name] = item
    fixed_control = summary['diagnostics']['t1-off0']['fixed']
    for name, item in summary['diagnostics'].items():
        changed = {k: [fixed_control.get(k), item['fixed'].get(k)]
                   for k in fixed_control.keys() | item['fixed'].keys()
                   if fixed_control.get(k) != item['fixed'].get(k)}
        item['corrected_control_differences'] = changed
        if name.startswith(('t1-', 't2-')):
            assert not changed, (name, changed)
    for phase in ['t1', 't2']:
        partition = ledger(f'{phase}-on0')
        off = statistics.mean(summary['diagnostics'][f'{phase}-{arm}']['optimizer_us'] for arm in ['off0', 'off1'])
        partition['on_vs_bracketed_off'] = summary['diagnostics'][f'{phase}-on0']['optimizer_us'] / off - 1
        summary['partitions'][phase] = partition
    for name in ['t0-d93f3b90', 't0-b3aa3365-pilot', 't0-4e29cd8f-pilot',
                 't0-b13d3872-pilot', 't2-final-pilot']:
        summary['normal'][name] = normal(name)
    (ROOT / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
