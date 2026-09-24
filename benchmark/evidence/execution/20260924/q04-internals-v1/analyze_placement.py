# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from argparse import Namespace
from pathlib import Path
import json
import sys

import duckdb
import psycopg

from benchmark_evidence import ImmutableDataSeed, isolated_paro_server, content_digest, repository_identity
from cold_planning import ProcessWatchdog
from tpcds_compare import configure_paro, run_paro, collect_statement_cache_evidence, snapshot_execution_ids
from tpcds_result_contract import duckdb_schema, assert_same_multiset, multiset_digest
from bound_result_contract import BoundResult, CATALOG_SQL, catalog_from_rows, order_values
from harness.executor import BenchmarkExecutor, _flatten_explain_profile, _build_explain_analyze_sql

ROOT = Path('/Users/linjunhong/workspace/paro')
QUERY_PATH = Path('/Users/linjunhong/workspace/duckdb/extension/tpcds/dsdgen/queries/04.sql')
SEED_PATH = Path('/private/tmp/paro-migration-relative.u1PLBV')
DB_PATH = Path('/Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb')
binary = ROOT / 'target/release/parod'
query = QUERY_PATH.read_text()
seed = ImmutableDataSeed.capture(SEED_PATH)
args = Namespace(optimizer_verify='off', optimizer_search_policy=sys.argv[1], threads=4,
                 memory_limit='2GB', statement_timeout_seconds=60,
                 disabled_optimizer_rules=sys.argv[2] if len(sys.argv) > 2 else '')

def emit(value):
    encoded = json.dumps(value, ensure_ascii=False)
    if len(encoded.encode()) > 32 * 1024 * 1024:
        raise RuntimeError('diagnostic record exceeds temporary 32 MiB limit')
    print(encoded, flush=True)

emit({'kind': 'identity', 'source': repository_identity(ROOT), 'binary_sha256': content_digest(binary),
      'sql_sha256': content_digest(QUERY_PATH), 'seed_sha256': seed.sha256,
      'duckdb_version': duckdb.__version__, 'configuration': vars(args),
      'claim': 'diagnostic-only; operator work is not additive wall time; association with normal execution is Uncovered',
      'temporary_capacity_bytes': 64 * 1024 * 1024})

executor = BenchmarkExecutor(connection={}, iterations=1, warmup=0, timeout_seconds=60, collect_memory=False)
with isolated_paro_server(binary, seed, '127.0.0.1:16433', None, max_memory='2GB', threads=4,
                          cache_evidence=True, optimizer_environment={
                              'PARO_COMPILE_WORK_EVIDENCE':'1',
                              'PARO_DIAGNOSTIC_SEARCH_STOP_MS':'30000',
                          }) as server:
    with ProcessWatchdog(server.process, 180, 3 * 1024 * 1024 * 1024) as watchdog:
        try:
            with psycopg.connect('host=127.0.0.1 port=16433 user=paro dbname=postgres', autocommit=True) as conn:
                configure_paro(conn, args)
                for regime in ('cold', 'warm'):
                    before = snapshot_execution_ids(conn)
                    raw = executor._fetch_explain_profile_json(conn, query)
                    _flatten_explain_profile(raw)
                    receipt = collect_statement_cache_evidence(conn, _build_explain_analyze_sql(query), before_execution_ids=before)
                    emit({'kind':'paro_profile', 'regime':regime, 'document':json.loads(raw), 'receipt':receipt})
                before = snapshot_execution_ids(conn)
                paro_rows, paro_schema = run_paro(conn, query, True)
                receipt = collect_statement_cache_evidence(conn, query, before_execution_ids=before)
                emit({'kind':'paro_select_receipt', 'receipt':receipt})
                plan = executor.execute_sql(conn, 'EXPLAIN '+query.rstrip().rstrip(';'), fetch=True)
                emit({'kind':'paro_plan', 'lines':[row[0] for row in plan]})
        finally:
            emit({'kind': 'watchdog', 'failure': watchdog.failure, 'peak_rss_bytes': watchdog.peak_rss})

with duckdb.connect(str(DB_PATH), read_only=True) as conn:
    conn.execute('SET threads=4')
    conn.execute("SET memory_limit='2GB'")
    for regime in ('cold','warm'):
        rows = conn.execute('EXPLAIN (ANALYZE, FORMAT JSON) '+query).fetchall()
        raw = next(row[1] for row in rows if row[0] == 'analyzed_plan')
        emit({'kind':'duckdb_profile', 'regime':regime, 'document':json.loads(raw)})
    cursor = conn.execute(query)
    duck_rows = cursor.fetchall()
    schema = duckdb_schema(cursor.description)
    catalog = catalog_from_rows(conn.execute(CATALOG_SQL).fetchall())
    with duckdb.connect() as parser:
        bound = BoundResult(query, parser, catalog)
        bound.check_identity(paro_schema,'paro')
        bound.check_identity(schema,'duckdb')
        bound.check_types(paro_schema,schema)
    expected = bound.canonical_rows(duck_rows,schema,'duckdb')
    actual = bound.canonical_rows(paro_rows,paro_schema,'paro')
    assert_same_multiset(actual,expected)
    assert order_values(actual,bound.bind_order(paro_schema,'paro')) == order_values(expected,bound.bind_order(schema,'duckdb'))
    emit({'kind':'result_validation','status':'passed','rows':len(actual),'sha256':multiset_digest(actual)})
