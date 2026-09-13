"""Fixed T1/control NI gate. Block medians, not the harness aggregate estimator."""
import gzip
import json
import math
import random
import statistics as s
import sys
from scipy.stats import t


def read(path):
    with gzip.open(path,'rt') if path.endswith('.gz') else open(path) as f:
        return json.load(f)


def interval(values):
    mean=s.mean(values)
    se=s.stdev(values)/math.sqrt(len(values))
    critical=t.ppf(.95,len(values)-1)
    return {'n':len(values),'ratio':math.exp(mean),
            'lower_one_sided95':math.exp(mean-critical*se),
            'upper_one_sided95':math.exp(mean+critical*se), 'log_sd':s.stdev(values)}


assert len(sys.argv[1:])==12
reports=[read(p) for p in sys.argv[1:]]
samples={'control':[],'prod':[]}
block_contrasts=[]
batch_contrasts=[]
for offset in range(0,12,2):
    pair={}
    for index in [offset,offset+1]:
        r=reports[index]
        assert r['source']['dirty'] is False
        role='control' if r['source']['commit'].startswith('6fce0fc0') else 'prod'
        assert role=='control' or r['source']['commit'].startswith('c23ae52a')
        q=r['queries'][0]
        assert q['status']=='passed' and q['rows']==90
        assert q['cold_statement']['trace_off_verified']
        rows=[]
        for b in q['process_blocks']:
            e=b['cold_miss_evidence']
            assert e['status']=='verified' and e['occurrence']==0 and e['cache_hit'] is False
            rows.append({'report':sys.argv[index+1], 'block':b['block'],
                         'W':s.median(b['paro_ms']), 'DW':s.median(b['duckdb_ms']),
                         'C1':b['cold_statement_ms']['paro'], 'DC1':b['cold_statement_ms']['duckdb'],
                         'raw_W':b['paro_ms'],'raw_DW':b['duckdb_ms']})
        assert len(rows)==6
        assert role not in pair
        pair[role]=rows
        samples[role].extend(rows)
    contrasts=[math.log(p['W']/c['W']) for c,p in zip(pair['control'],pair['prod'])]
    block_contrasts+=contrasts
    batch_contrasts.append(s.mean(contrasts))
block=interval(block_contrasts)
batch=interval(batch_contrasts)
rng=random.Random(0)
bootstrap=sorted(math.exp(s.mean(rng.choices(block_contrasts,k=36))) for _ in range(10000))
summary={}
for role,rows in samples.items():
    summary[role]={m:{'median':s.median(r[m] for r in rows),
                      'p95_observed':sorted(r[m] for r in rows)[round(.95*(len(rows)-1))]}
                   for m in ['W','DW','C1','DC1']}
    summary[role]['PD_W']=interval([math.log(r['W']/r['DW']) for r in rows])
    summary[role]['PD_C1']=interval([math.log(r['C1']/r['DC1']) for r in rows])
print(json.dumps({'samples':samples,'summary':summary,'NI_block':block,'NI_batch':batch,
                  'pair_bootstrap95_upper':bootstrap[9499],
                  'NI_certified':block['upper_one_sided95']<=1 and batch['upper_one_sided95']<=1,
                  'worse_supported':block['lower_one_sided95']>1 and batch['lower_one_sided95']>1,
                  'power_warning':'N36 conditional P/D=.88 design does not imply90% power for T1/control zero-margin NI. No tail/cross-family certification.'},indent=2))
