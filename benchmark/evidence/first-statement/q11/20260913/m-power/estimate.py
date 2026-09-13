"""Conditional sample-size analysis of archived process blocks, no engine changes."""
import gzip
import hashlib
import json
import math
import statistics as s
from pathlib import Path
from scipy.stats import chi2, nct, t

root = Path(__file__).resolve().parent.parent
strata = {
    'E1baseline': ['e1-cold-t1-prod/raw/paro-tcwc-e1-cold-baseline-handoff'],
    'E1off': ['e1-cold-t1-prod/raw/paro-tcwc-e1-cold-off-handoff'],
    'E1v2off': ['e1-cold-t1-prod/raw/paro-tcwc-e1-cold-v2-off-handoff'],
    'T1': ['e1-cold-t1-prod/raw/paro-tcwc-t1-prod-handoff'],
    'E2normal': ['e2-decouple/raw/paro-tcwc-e2-control-a', 'e2-decouple/raw/paro-tcwc-e2-control-b'],
    'E2touch_W_only': ['e2-decouple/raw/paro-tcwc-e2-touch-a', 'e2-decouple/raw/paro-tcwc-e2-touch-b'],
    'E3repeat': ['e3-admit/raw/paro-e3-r-control-a', 'e3-admit/raw/paro-e3-r-control-b'],
}
result = {}
for name, paths in strata.items():
    blocks = []
    inputs = []
    for path in paths:
        path = root/(path+'.json.gz')
        r = json.loads(gzip.decompress(path.read_bytes()))
        assert r['source']['dirty'] is False
        inputs.append({'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                       'source': r['source'], 'binary': r['build_attestation']['binary_sha256']})
        for b in r['queries'][0]['process_blocks']:
            lp, ld = map(math.log, [s.median(b['paro_ms']), s.median(b['duckdb_ms'])])
            cp, cd = map(math.log, [b['cold_statement_ms']['paro'], b['cold_statement_ms']['duckdb']])
            blocks.append({'W_logP':lp, 'W_logD':ld, 'W_logratio':lp-ld,
                           'C1_logP':cp, 'C1_logD':cd, 'C1_logratio':cp-cd,
                           'P_warm_samples': b['paro_ms'], 'D_warm_samples': b['duckdb_ms']})
    stats = {}
    for metric in ['W', 'C1']:
        if metric == 'C1' and 'W_only' in name:
            continue
        p,d,x = ([b[metric+k] for b in blocks] for k in ['_logP', '_logD', '_logratio'])
        stats[metric] = {'log_mean':s.mean(x), 'ratio':math.exp(s.mean(x)),
                         'log_sd':s.stdev(x), 'log_varP':s.variance(p), 'log_varD':s.variance(d),
                         'log_covPD':s.covariance(p,d),
                         'sigma_upper95_normal':s.stdev(x)*math.sqrt((len(x)-1)/chi2.ppf(.05,len(x)-1))}
    result[name] = {'inputs':inputs, 'blocks':blocks, 'stats':stats}
sigma = max(v['stats']['W']['sigma_upper95_normal'] for v in result.values())
curve = []
for n in range(4, 1001, 4):
    power = nct.cdf(-t.ppf(.95,n-1), n-1, math.log(.88)*math.sqrt(n)/sigma)
    curve.append({'n':n,'power':float(power)})
    if power >= .90:
        break
print(json.dumps({'strata':result, 'blocks':sum(len(v['blocks']) for v in result.values()),
                  'conservative_sigma':sigma, 'power_curve':curve,
                  'conditional_balanced_n':curve[-1]['n'],
                  'assumptions':'Independent stationary normal log block ratios; alpha .05 one-sided; true ratio .88; power .90. Not a universal or tail-safe minimum. T1/control zero-margin NI has different effect and covariance; no power guarantee follows.'},indent=2))
