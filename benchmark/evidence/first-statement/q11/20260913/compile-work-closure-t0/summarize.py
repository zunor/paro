"""Recompute T-CWC normal per-occurrence residuals; no cross-cohort subtraction."""
import gzip
import json
import re
import statistics
from pathlib import Path

root = Path(__file__).parent
summaries = []
for path in sorted(root.glob('paro-tcwc-t[013]-*.json.gz')):
    with gzip.open(path, 'rt') as stream:
        report = json.load(stream)
    query = report['queries'][0]
    cold = query['cold_statement']
    blocks = []
    for block in query['process_blocks']:
        work = block['cold_miss_evidence'].get('compile_work', {})
        item = {'block':block['block'], 'occurrence':block['cold_miss_evidence']['occurrence'],
                'C1_ms':block['cold_statement_ms']['paro'],
                'W_ms':statistics.median(block['paro_ms']), **work}
        if work:
            item['U_residual_us_not_synthesis_time'] = (
                work['optimizer_elapsed_us'] - work['rule_elapsed_us']
            ) / work['child_combination_cost_synthesis_count']
            item['cold_warm_residual_ms_not_stage_time'] = (
                item['C1_ms'] - item['W_ms'] - work['compiler_elapsed_us']/1000
            )
        blocks.append(item)
    events = [event for block in query['diagnostic_cohort']['process_blocks']
              for trace in block['statement_traces'] for event in trace['events']]
    names = {'first_safe_us','published_winner_count','quality_policy_satisfied_us',
             'child_combination_cost_synthesis_count','child_combination_recompute_count',
             'child_combination_frontier_recheck_count','child_combination_new_count',
             'memo_group_count','memo_logical_expression_count','memo_physical_expression_count',
             'quality_policy_satisfied','search_stop_budget_limited','search_incomplete',
             'governor_search_complete','search_actual_stop_us'}
    diagnostic = {name: [e['value'] for e in events if e['event']==name] for name in sorted(names)}
    log = root / path.name.replace('.json.gz','.q11.diagnostic000.parod.log.gz')
    admission = []
    if log.exists():
        with gzip.open(log, 'rt') as stream:
            for line in stream:
                if 'admission_identity' in line:
                    plain = re.sub(r'\x1b\[[0-9;]*m','',line)
                    match = re.search(r'class=(\d+) physical_fingerprint=([0-9a-f]+)',plain)
                    if match:
                        admission.append({'class':int(match[1]),'fingerprint':match[2]})
    summaries.append({'report':path.name, 'source':report['source'],
                      'binary_sha256':report['build_attestation']['binary_sha256'],
                      'status':query['status'], 'rows':query['rows'],
                      'SQL_sha256':report['query_corpus_sha256'],
                      'C1':cold['paro'],'DuckDB_C1':cold['duckdb'],
                      'C1_ratio':cold['crossover'],'W':query['paro'],
                      'DuckDB_W':query['duckdb'],'W_ratio':query['crossover'],
                      'normal_blocks':blocks,'diagnostic_only':diagnostic,
                      'diagnostic_admission_attempts_not_normal_image_attestation':admission})
print(json.dumps(summaries,indent=2))
