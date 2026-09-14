#!/usr/bin/env python3
"""One clean-source Q11 diagnostic block using the existing corpus contracts.

SELECT is first and supplies authoritative frozen choices/admission evidence.
Optional EXPLAIN is a later diagnostic, never a C1 or identical-image claim.
"""
import argparse
import dataclasses
import gzip
import json
import os
from pathlib import Path
import re
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--sql', type=Path, required=True)
    parser.add_argument('--seed', type=Path, required=True)
    parser.add_argument('--duckdb', type=Path, required=True)
    parser.add_argument('--explain', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo / 'benchmark/corpora'))
    import tpcds_compare as h
    from cold_planning import ProcessWatchdog

    source = h.repository_identity(args.repo)
    assert source['dirty'] is False, source
    binary, build = h.build_benchmark_server(args.repo, 4)
    seed = h.ImmutableDataSeed.capture(args.seed)
    query = args.sql.read_text()
    fingerprint = h.statement_fingerprint(query)
    config = argparse.Namespace(listen='127.0.0.1:16463', database='postgres', user='paro',
                                threads=4, memory_limit='2GB', statement_timeout_seconds=300)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    log = args.out.with_suffix('.parod.log')
    env = {'PARO_QUALITY_POLICY_HANDOFF': '1', 'PARO_COMPILE_WORK_EVIDENCE': '1'}
    with h.isolated_paro_server(binary, seed, config.listen, log, max_memory='2GB',
                               threads=4, statement_trace=True, trace_sample_id=args.out.name,
                               cache_evidence=True, optimizer_environment=env) as server:
        with ProcessWatchdog(server.process, 300, 2 * 1024**3) as watchdog:
            connection = h.open_paro_connection(config)
            try:
                rows, schema, elapsed = h.timed_run_paro(connection, query, True)
                miss = h.collect_statement_cache_evidence(connection, query)
                assert miss['status'] == 'verified' and miss['occurrence'] == 0 and not miss['cache_hit'], miss
                # This is deliberately after first SELECT and separate from its trace.
                explain = None
                if args.explain:
                    explain = connection.execute('EXPLAIN ' + query.strip().rstrip(';') + ' FORMAT JSON').fetchall()
                metadata = h.paro_metadata_inventory(connection)
                server_identity = server.identity()
            finally:
                connection.close()
        assert watchdog.failure is None, watchdog.failure
    with h.DuckDBProcess(args.duckdb, 4, '2GB') as duck:
        oracle_rows, oracle_schema, _ = duck.execute(query)
        duck_metadata = h.duckdb_metadata_inventory(duck)
    h.assert_compatible_schema(schema, oracle_schema)
    actual = h.canonicalize_rows(rows, schema)
    expected = h.canonicalize_rows(oracle_rows, oracle_schema)
    h.assert_same_multiset(actual, expected)
    order = h.parse_order_contract(query, oracle_schema)
    assert h.assert_peer_order(actual, order) == h.assert_peer_order(expected, order)
    symmetric = h.validate_metadata_track('generator-declared', metadata, duck_metadata)
    traces = h.parse_statement_trace_log(log)
    targets = [t for t in traces if t['query_fingerprint'] == fingerprint]
    assert len(targets) == 1
    h.validate_statement_trace(targets[0], expected_process_id=server_identity['pid'],
                               expected_sample_id=args.out.name, expected_query_fingerprint=fingerprint)
    values = {e['event']: e['value'] for e in targets[0]['events'] if e['value'] is not None}
    clean_log = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', log.read_text())
    admissions = re.findall(r'diagnostic portfolio admission choice.*physical_fingerprint=([a-f0-9]+)', clean_log)
    assert admissions
    triple = dict(admitted_fingerprint=admissions[0],
                  syntheses=values['child_combination_cost_synthesis_count'],
                  published=values['published_winner_count'])
    assert h.repository_identity(args.repo) == source == build['source']
    assert h.content_digest(binary) == build['binary_sha256']
    result = dict(cohort='diagnostic_only', normal_c1=False, source=source, build=build,
                  sql_sha256=h.content_digest(args.sql), query_fingerprint=fingerprint,
                  seed_sha256=seed.sha256, duckdb_sha256=h.content_digest(args.duckdb),
                  harness={str(p): h.content_digest(p) for p in [Path(__file__),
                      args.repo / 'benchmark/corpora/tpcds_compare.py',
                      args.repo / 'benchmark/corpora/benchmark_evidence.py',
                      args.repo / 'benchmark/corpora/tpcds_result_contract.py',
                      args.repo / 'benchmark/corpora/cold_planning.py']},
                  optimizer_environment=env, process=server_identity, client_ms=elapsed,
                  miss=miss, rows=len(rows), schema=[dataclasses.asdict(c) for c in schema],
                  result_digest=h.multiset_digest(actual), metadata_symmetric=symmetric,
                  triple=triple, values=values, traces=traces, later_explain=explain,
                  log_sha256=h.content_digest(log), peak_rss_bytes=watchdog.peak_rss)
    args.out.with_suffix('.json.gz').write_bytes(gzip.compress(json.dumps(result, sort_keys=True).encode(), mtime=0))
    print(json.dumps(dict(source=source['commit'], triple=triple, rows=len(rows)), sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
