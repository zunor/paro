# Canonical construction, selected DAG and physical observations

EvidenceId: construction-dag-v1. Registration:
[construction-dag-registration.md](../construction-dag-registration.md).

## Result

All three ownership/construction changes are implemented, without changing
search budgets, stop policy, costing, execution or verification settings.
This pilot does **not** show a broad compiler speedup. Q11 improved modestly;
Q04/Q74 were essentially flat. It does not certify warm non-inferiority,
compiler <10ms, first-statement parity or search completeness.

| Query | Control compiler median / P90 (ms) | Probe compiler median / P90 (ms) | Median change | Cost syntheses (both) |
| --- | ---: | ---: | ---: | ---: |
| Q04 | 19.626 / 25.208 | 20.117 / 21.388 | +2.5% | 782 |
| Q11 | 13.169 / 14.477 | 12.476 / 13.352 | -5.3% | 401 |
| Q74 | 40.696 / 43.588 | 41.142 / 42.668 | +1.1% | 1045 |

| Query | Control → probe C1 median (ms) | Control → probe warm median (ms) |
| --- | ---: | ---: |
| Q04 | 305.352 → 289.179 | 172.814 → 175.845 |
| Q11 | 194.729 → 167.003 | 102.667 → 97.353 |
| Q74 | 150.876 → 149.905 | 58.016 → 66.705 |

These are descriptive medians from six fresh blocks per arm/query. C1 changes
are not attributed to the much smaller compiler delta. All slow samples are
retained. The two warm occurrences in a block are not independent processes.
The maintained cross-engine block bootstrap results are in summary.json;
they do not turn this source comparison into a paired speedup estimate.

All normal receipts are verified (36 fresh cache misses and 72 cache hits).
Every query has identical artifact, selection and resource identities across
both source arms; complete typed results were independently checked on every
sample. Groups/logical/physical counts and synthesis/proposal counts are also
unchanged. Fingerprints locate these products; they are not substitutes for
the SQL-result oracle. Both arms stop at QualityPolicySatisfied with
search_complete=false, not ProofComplete.

## Implemented contracts

- CanonicalScalars remembers only completed operator-root expressions via weak
  allocation witnesses. Unchanged roots skip the second scalar rewrite;
  mutation detaches the identity. Root/child rule context and volatile
  occurrences are not merged. Aggregate output types are still recomputed.
  The final empty-result/restriction laws share one local postorder;
  nonlocal CTE/filter barriers keep their existing order.
- SelectedDagStore owns immutable exact CandidateId edge lists and only one
  current transitive view, avoiding a closure per historical root. Incoming
  edges and iterative postorder are constructed once for that root. Fact
  changes invalidate the changed node and its selected ancestors; group-key
  rewrites are observed too. Only demanded region boundaries cache scalar
  certificates. Unchanged payload structure is reused, not stale facts.
- CurrentReadSet is an ephemeral observation tied to an immutable Memo borrow.
  Physical resume carries it into TaskRegistry instead of rescanning the same
  cursors. Resident tasks still own revision-sensitive ReadSets. Child search
  mutations require a new observation before publication; completion,
  mandatory/optional phase and recipe-cursor checks remain independent.

Final safety validation remains in place. Independent oracles cover the old
separate-pass normalization, frozen selected evidence, fact/context changes,
shared incoming edges, redirects, volatile roots, holes/cycles, and a
10,000-edge selected DAG on a small native stack. No legacy production path,
new experiment flag, budget reduction or query-specific case was introduced.

## Attribution and the warm investigation

Q04/Q11 derive selected properties only once in these runs (55/37 node builds,
zero node reuses). They offer little selected-DAG reuse to monetize. Q74 has
423 builds and 4,644 reuses in **both** arms: baseline property reuse already
existed, and this task primarily saves structural work around it.

Separate Q74 Detail QualityEvidence observations are 12.486/10.800ms in
control and 10.680/10.851ms in probe. This is not a repeated large reduction,
and Detail timings are never used as normal compiler timings. Requests,
evaluations, ReadSet rebuilds and cost syntheses are unchanged. Thus the new
ownership contracts are useful foundations, but this evidence does not
justify claiming they removed the remaining critical path.

Q74 warm +15.0% crosses the registered +10% investigation threshold:
all warm receipts are CacheHit, exact selected/resource identities match,
and this change does not modify execution code. Individual probe blocks and
DuckDB timings also vary; neither that observation nor plan identity proves
the cause is environmental. No cause was isolated, no slow sample was
discarded, and no repeated-until-green replacement cohort was collected.
Warm non-inferiority remains NotCertified.

Further optimization should target remaining repeated live-payload/fact
inspection measured in QualityEvidence, not add another cache or widen this
change without demonstrating actual repeated work. Mandatory final validation
cannot be replaced by a reuse counter.

## Reproduction and evidence ownership

Control: 9536a4df (registration-only successor of df6627ea).
Probe: 295d487b. All collections used clean source snapshots.
Implementation commits: 53739194, 42965603, d0dcdece, 295d487b.

One checkout and one shared target were used sequentially, C/P/C/P.
Each batch visited Q04/Q11/Q74 with three normal fresh process blocks,
one warmup and one ABBA round per block, plus one separate Detail block/query.
Round seeds: 2026092305 and 2026092306. Resources: four execution threads,
2GB, binary results, quality policy, optimizer_verify=off. The optional
deadline remains the unchanged 30 seconds; no override was set.
PARO_COMPILE_WORK_EVIDENCE=1 is the sole normal compile observer.

Inputs and exact source/build identities are recorded by the maintained
collector in each matrix/*/inputs.json and summarized in summary.json.
DuckDB is the declared 1.5.5 binary/extension identity. Metadata is
generator-declared; this pilot is not a calibrated cost-model/parity gate.

The command per cell was:

```sh
PYTHONPATH=benchmark PARO_COMPILE_WORK_EVIDENCE=1 \
  benchmark/.venv/bin/python benchmark/corpora/tpcds_compare.py \
  --server-data-dir /private/tmp/paro-migration-relative.u1PLBV \
  --duckdb-database /Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb \
  --dataset-source-dir /Users/linjunhong/workspace/tpcds-sf1/csv \
  --query-dir /Users/linjunhong/workspace/duckdb/extension/tpcds/dsdgen/queries \
  --report <owned-output>/<arm>-<round>-q<query>.json \
  --start <query> --end <query> --listen 127.0.0.1:16433 \
  --process-blocks 3 --diagnostic-process-blocks 1 \
  --measurement-rounds-per-process 1 --warmups-per-process 1 \
  --bootstrap-samples 10000 --random-seed <registered-round-seed> \
  --threads 4 --memory-limit 2GB --optimizer-search-policy quality \
  --optimizer-verify off --metadata-track generator-declared \
  --paro-result-format binary --build-jobs 4
```

The archive keeps all twelve bounded RunOutput directories unchanged, with
accepted attempts and one capture reference each. No event flood, binary,
dataset copy or server text log is archived. Total evidence is below the
registered 16MiB cap. SHA256SUMS excludes only itself.

## Validation

- Workspace Rust tests: 6,940 passed, zero failures, 85 existing ignored.
  Optimizer: 1,401 passed, zero failures.
- Workspace check and strict Clippy: passed.
- Benchmark harness: 207 passed; regress harness: 103 passed, one skipped.
- Memory/vector API guards: passed.
- Compare-only full SQL regress, verifier on, FD 65,536:
  **185 passed, zero failed, zero expected updates**.
- Campaign/cell validators and capture hashes: passed; original ignored
  regress report restored and owned test service stopped.

See validation.json and sql-regress.txt. No performance target or completeness
status was upgraded by these correctness checks.
