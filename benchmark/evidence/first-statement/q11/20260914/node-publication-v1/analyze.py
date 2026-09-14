#!/usr/bin/env python3
"""Archive/recompute this experiment; retain failed and withdrawn runs."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
REPO = next(p for p in ROOT.parents if (p / 'Cargo.toml').is_file())
sys.path.insert(0, str(REPO / 'benchmark/corpora'))
import tpcds_compare as h


def read(name):
    return json.loads(gzip.decompress((ROOT / 'raw' / name).read_bytes()))


def diagnostic(name):
    d = read(f'paro-node-publication-{name}.json.gz')
    assert not d['source']['dirty'] and d['rows'] == 90
    assert d['miss']['status'] == 'verified' and d['miss']['occurrence'] == 0
    assert not d['miss']['cache_hit']
    trace = next(t for t in d['traces'] if t['query_fingerprint'] == d['query_fingerprint'])
    v = d['values']
    fixed = {k: x for k, x in v.items() if k.startswith('final_winner_2.')
             or ('.' not in k and k.endswith('_count') and 'lifecycle' not in k)
             or (k.startswith('rule.') and not any(s in k for s in ('_us', '_ns', 'allocated')))}
    return dict(source=d['source'], build=d['build'], triple=d['triple'], fixed=fixed,
                optimizer_us=next(e['duration_us'] for e in trace['events'] if e['event'] == 'optimizer'),
                quality_us=v.get('quality_policy_satisfied_us'),
                search_complete=v.get('governor_search_complete'))


def partition(name):
    path = ROOT / 'raw' / f'paro-node-publication-{name}.ledger.jsonl.gz'
    row = json.loads(gzip.decompress(path.read_bytes()).splitlines()[0])
    d = read(f'paro-node-publication-{name}.json.gz')
    assert row['pid'] == d['process']['pid'] and row['success']
    assert row['total_ns'] == row['sum_ns'] == sum(b['exclusive_ns'] for b in row['buckets'])
    b3 = [b for b in row['buckets'] if b['bucket'].startswith('B3')]
    total = sum(b['exclusive_ns'] for b in b3)
    assert total == sum(sum(r['ns']) for r in row['b3_by_rule'].values())
    row['b3_coverage'] = 1 - b3[0]['exclusive_ns'] / total
    for b in row['buckets']:
        b['us_per_interval'] = b['exclusive_ns'] / 1000 / b['entries'] if b['entries'] else None
    return row


def normal(names):
    blocks, origins = [], []
    for name in names:
        d = read(f'paro-node-publication-{name}.json.gz')
        assert not d['source']['dirty'] and d['failed'] == 0
        q = d['queries'][0]
        assert q['rows'] == 90 and q['cold_statement']['trace_off_verified']
        assert q['cold_statement']['primary_gate_eligible']
        origins.append(dict(report=name, source=d['source'], build=d['build_attestation']))
        for block in q['process_blocks']:
            miss = block['cold_miss_evidence']
            assert miss['status'] == 'verified' and miss['occurrence'] == 0 and not miss['cache_hit']
            blocks.append(dict(block, source_report=name, source_block=block['block'], block=len(blocks)))
    return dict(origins=origins, blocks=blocks,
                c1={engine: h.timing_summary([b['cold_statement_ms'][engine] for b in blocks])
                    for engine in ('paro', 'duckdb')},
                warm={engine: h.timing_summary([v for b in blocks for v in b[engine + '_ms']])
                      for engine in ('paro', 'duckdb')},
                cold_ratio=h.hierarchical_cold_ratio(blocks), warm_ratio=h.hierarchical_abba_ratio(blocks, 10000),
                compiler=h.timing_summary([b['cold_miss_evidence']['compile_work']['compiler_elapsed_us'] / 1000 for b in blocks]),
                optimizer=h.timing_summary([b['cold_miss_evidence']['compile_work']['optimizer_elapsed_us'] / 1000 for b in blocks]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', action='store_true')
    args = parser.parse_args()
    raw = ROOT / 'raw'
    if args.archive:
        raw.mkdir(exist_ok=True)
        manifest = []
        for p in sorted(Path('/private/tmp').glob('paro-node-publication-*')):
            if not p.is_file() or p.suffix not in ('.json', '.jsonl', '.gz', '.log'):
                continue
            original = p.read_bytes()
            content = gzip.decompress(original) if p.suffix == '.gz' else original
            data = gzip.compress(content, mtime=0)
            target = raw / (p.name if p.suffix == '.gz' else p.name + '.gz')
            if target.exists() and target.read_bytes() != data:
                raise ValueError(f'artifact changed: {target}')
            target.write_bytes(data)
            manifest.append(dict(file=target.name, raw_bytes=len(content), raw_sha256=hashlib.sha256(content).hexdigest(),
                                 gzip_sha256=hashlib.sha256(data).hexdigest()))
        (raw / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    result = dict(diagnostics={}, partitions={}, normal={})
    for p in sorted(raw.glob('paro-node-publication-*.json.gz')):
        d = json.loads(gzip.decompress(p.read_bytes()))
        if d.get('cohort') == 'diagnostic_only':
            name = p.name.removeprefix('paro-node-publication-').removesuffix('.json.gz')
            result['diagnostics'][name] = diagnostic(name)
    control = result['diagnostics']['control-off']['fixed']
    for name, d in result['diagnostics'].items():
        d['control_differences'] = {k: [control.get(k), d['fixed'].get(k)]
                                    for k in control.keys() | d['fixed'].keys() if control.get(k) != d['fixed'].get(k)}
        if (raw / f'paro-node-publication-{name}.ledger.jsonl.gz').is_file():
            result['partitions'][name] = partition(name)
    result['normal']['registered_control'] = normal(['control-normal'])
    result['normal']['registered_probe'] = normal(['probe-a', 'probe-b'])
    if (raw / 'paro-node-publication-final-normal.json.gz').is_file():
        result['normal']['final_verification_not_pooled'] = normal(['final-normal'])
    (ROOT / 'summary.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
