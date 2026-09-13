"""Recompute same-block cold/warm execution scalars. Never convert faults to ms."""
import gzip
import json
import statistics
import sys
from pathlib import Path

summaries = []
for arg in sys.argv[1:]:
    path = Path(arg)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        report = json.load(stream)
    query = report['queries'][0]
    blocks = []
    for block in query['process_blocks']:
        cache = block['cold_miss_evidence']
        compiler = cache['compile_work']['compiler_elapsed_us']/1000
        c1 = block['cold_statement_ms']['paro']
        warm = statistics.median(block['paro_ms'])
        result = {'block':block['block'], 'C1_ms':c1, 'W_ms':warm, 'compiler_ms':compiler,
                  'cold_warm_residual_ms_not_phase':c1-warm-compiler}
        cold = cache.get('execution_work', {})
        warms = block.get('paro_execution_work', [])
        if cold and warms:
            cm = cold['metrics']
            wm = {key:statistics.median([record['metrics'][key] for record in warms]) for key in cm}
            result.update({'cold_execution':cold,'warm_executions':warms,
                'same_image':all(record['metrics']['image_id']==cm['image_id'] for record in warms),
                'distinct_execution_ids':len({cold['execution_id'], *[r['execution_id'] for r in warms]})==len(warms)+1,
                'execution_cold_warm_delta_ms':(cm['execution_elapsed_us']-wm['execution_elapsed_us'])/1000,
                'cold_minus_warm_metrics':{key:cm[key]-wm[key] for key in cm if key!='image_id'}})
            names = ['buffer_fill','decoder','dictionary','zone_map','global_init','local_init']
            result['positive_exclusive_worker_delta_ms_not_wall'] = sum(max(0,cm[name+'_exclusive_worker_ns']-wm[name+'_exclusive_worker_ns']) for name in names)/1e6
            result['attribution_warning'] = 'Faults/RSS have no measured latency; worker elapsed sums are not critical-path wall. Do not claim 80% from their ratio.'
            masks = [f'activity_mask_{mask}_wall_ns' for mask in range(64)]
            if all(key in cm for key in masks):
                cold_union = sum(cm[key] for key in masks[1:])
                warm_union = statistics.median(sum(record['metrics'][key] for key in masks[1:]) for record in warms)
                result['activity_wall'] = {
                    'cold_union_ms': cold_union/1e6,
                    'warm_union_ms': warm_union/1e6,
                    'union_delta_ms': (cold_union-warm_union)/1e6,
                    'uncovered_delta_ms': (cm[masks[0]]-wm[masks[0]])/1e6,
                    'warning': 'Temporal occupancy, not causal critical-path attribution. Mixed masks remain overlapping and unallocated; faults cannot be added as latency.',
                }
        blocks.append(result)
    summaries.append({'report':str(path),'source':report['source'],
        'binary_sha256':report['build_attestation']['binary_sha256'],
        'C1':query['cold_statement']['paro'],'W':query['paro'],
        'C1_ratio':query['cold_statement']['crossover'],'W_ratio':query['crossover'],
        'rows':query['rows'],'status':query['status'],'blocks':blocks})
print(json.dumps(summaries,indent=2))
