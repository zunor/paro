"""T-CWC diagnostic only: sample one fresh SELECT, never a normal C1 result."""
import argparse
import json
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

from benchmark_evidence import ImmutableDataSeed, isolated_paro_server, content_digest
from tpcds_compare import open_paro_connection, timed_run_paro, collect_statement_cache_evidence

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
args = parser.parse_args()
out = Path(args.output)
root = Path('/private/tmp/paro-tcwc-baseline-ZjwFtb')
binary = root / 'target/release/parod'
query = (root / 'benchmark/evidence/first-statement/q11/20260910/early-stop-v1/11.sql').read_text()
seed = ImmutableDataSeed.capture(Path('/private/tmp/paro-necessary-domain-seed-direct-v1'))
settings = SimpleNamespace(listen='127.0.0.1:16433', database='postgres', user='paro',
                           threads=4, memory_limit='2GB', statement_timeout_seconds=300)
with isolated_paro_server(binary, seed, settings.listen, out.with_suffix('.parod.log'),
                          max_memory='2GB', threads=4, cache_evidence=True,
                          optimizer_environment={'PARO_COMPILE_WORK_EVIDENCE':'1',
                                                 'PARO_DIAGNOSTIC_LAZY_GRANT':None,
                                                 'PARO_QUALITY_POLICY_HANDOFF':None}) as server:
    with open_paro_connection(settings) as connection:
        profiler = subprocess.Popen(['/usr/bin/sample', str(server.process.pid), '3', '1',
                                     '-file', str(out.with_suffix('.sample.txt'))])
        time.sleep(.15)
        rows, schema, elapsed = timed_run_paro(connection, query, True)
        code = profiler.wait(timeout=20)
        evidence = collect_statement_cache_evidence(connection, query)
        print(json.dumps({'diagnostic_only':True, 'profiler_exit_code':code,
                          'rows':len(rows), 'client_ms':elapsed, 'compile_work':evidence,
                          'binary_sha256':content_digest(binary), 'seed_sha256':seed.sha256,
                          'source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()}))
