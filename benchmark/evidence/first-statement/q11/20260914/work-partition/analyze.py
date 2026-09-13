#!/usr/bin/env python3
"""Same-occurrence exclusive accounting, never cross-cohort subtraction."""
import argparse
import gzip
import json
import statistics
from pathlib import Path


def read(path):
    return gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()


def analyze(report_path, ledger_path=None):
    d = json.loads(read(report_path))
    assert not d['source']['dirty'] and d['failed'] == 0
    assert d['query_corpus_sha256'] == 'a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503'
    q = d['queries'][0]
    assert q['rows'] == 90
    ledger = [json.loads(line) for line in read(ledger_path).splitlines()] if ledger_path else []
    blocks = []
    for b in q['process_blocks']:
        miss = b['cold_miss_evidence']
        assert miss['status'] == 'verified' and miss['occurrence'] == 0 and not miss['cache_hit']
        work = miss['compile_work']
        item = dict(block=b['block'], server=b['paro_server'], work=work,
                    c1=b['cold_statement_ms'], warm=b['paro_ms'])
        if ledger_path:
            # Identical optimizer end/start Instants, integer microsecond sidecar.
            matches = [r for r in ledger if r['pid'] == b['paro_server']['pid']
                       and r['total_ns'] // 1000 == work['optimizer_elapsed_us']]
            assert len(matches) == 1, (work, len(matches))
            r = matches[0]
            assert r['success'] and r['total_ns'] == r['sum_ns'] == sum(x['exclusive_ns'] for x in r['buckets'])
            item['partition'] = r
            item['coverage'] = 1 - r['buckets'][-1]['exclusive_ns'] / r['total_ns']
            for bucket in r['buckets']:
                bucket['us_per_entry'] = bucket['exclusive_ns'] / 1000 / bucket['entries'] if bucket['entries'] else None
        blocks.append(item)
    events = q['diagnostic_cohort']['process_blocks'][0]['target_statement_traces'][0]['events']
    values = {e['event']: e['value'] for e in events if e['value'] is not None}
    result = dict(source=d['source'], attestation=d['build_attestation'], blocks=blocks,
                  cohort='diagnostic_partition_trace_off' if ledger_path else 'normal_trace_off',
                  primary_c1_eligible=ledger_path is None,
                  optimizer_median_us=statistics.median(b['work']['optimizer_elapsed_us'] for b in blocks),
                  c1=q['cold_statement'], warm=q['paro'], warm_ratio=q['warm_paro_over_duckdb'],
                  diagnostic_counters={k: v for k, v in values.items() if 'count' in k or k.startswith('rule.')},
                  diagnostic_fingerprints={k: v for k, v in values.items() if k.startswith('final_winner_') and 'fingerprint' in k},
                  quality_ready_us=values.get('quality_policy_satisfied_us'),
                  complete=values.get('governor_search_complete'))
    if ledger_path:
        result['diagnostic_partition'] = [r for r in ledger if r['pid'] == q['diagnostic_cohort']['process_blocks'][0]['paro_server']['pid']]
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('report', type=Path)
    parser.add_argument('--ledger', type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.report, args.ledger), indent=2))
