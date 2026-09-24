# Q04 DECIMAL fusion: interrupted fresh comparison

Registration: [q04-c1-round2](../q04-c1-round2-registration.md), committed in
`c8b04fe9` before sampling. **Performance conclusion: NotCertified.**
Both collected batches completed and passed correctness/receipt validation;
the registered four-batch comparison did not finish because unrelated host
work reappeared. Collection completion is not environmental validity.

## Sources and collection

- Control: clean `328dd1b4` (runtime code identical to `8dfc2b1a`). Binary:
  `2d69f746ab54b34d8db8a8cea2df8f05b73d3f5f7f16ab7f7bd26e3b4b105b95`.
- Probe: clean `c8b04fe9` (runtime code `397fa88f` / `c7e02c67`, same as
  `a79caf5b`). Binary:
  `a13d084667d1971b837547fded00c0c5e83f8196a6fa8d7f20abc40fe2b5b29a`.
- DuckDB 1.5.5, SF1, four threads, 2GB, binary results, quality policy,
  optimizer verifier off. Both arms explicitly used
  `PARO_COMPILE_WORK_EVIDENCE=1` and `PARO_DIAGNOSTIC_SEARCH_STOP_MS=30000`.
  Normal samples were trace-off; bounded Detail ran in a separate process.
- Same immutable seed, SQL, CSV inputs, Python/engine/extension identities,
  schema and generator-declared metadata; full identities and actual command
  arguments are retained in each maintained collector's `inputs.json`.
  Metadata is asymmetric to DuckDB, independently precluding parity certification.
- Planned C/P/C/P, three fresh normal blocks and one diagnostic process per
  batch. Collected **C1/P1 only**, seed `2026092405`: three normal blocks per
  arm, one warmup and one ABBA warm round per process. No retry or replacement
  batch. C2/P2 were not started, rather than silently counted as completed.

The maintained campaigns are retained byte-for-byte in
[control-1-run](control-1-run/manifest.json) and
[probe-1-run](probe-1-run/manifest.json), including all valid slow samples,
receipts, result digests, accepted attempts and one capture per diagnostic cell.
No free-text server log, binary or raw stack sample is archived. The entire
archive is below the registered 5 MiB bound.

## Observations, not an attributed speedup or regression

All values below are milliseconds. Medians use the complete collected samples;
warm has six timed observations nested in three independent process blocks.

| Collected batch | Paro C1 | DuckDB C1 | Paro compiler | Paro warm | DuckDB warm |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control 1 | 247.316 | 130.741 | 19.724 | 173.192 | 127.186 |
| Probe 1 | 307.412 | 217.679 | 20.859 | 291.642 | 215.207 |

Cold samples in acquisition order:

- Control Paro: 242.511458, 247.316417, 323.715625.
- Control DuckDB: 119.662541, 130.741125, 142.734875.
- Probe Paro: 280.490083, 307.411584, **964.722166**.
- Probe DuckDB: 205.213750, 217.678667, **839.333542**.
- Normal compiler control: 19.724, 19.063, 21.133.
- Normal compiler probe: 19.975, 20.859, **41.842**.

The maintained process-level cold bootstrap reports Paro/DuckDB ratio 2.056282
[1.891650, 2.267950] for control and 1.304251 [1.149391, 1.412227] for probe.
These are descriptions of the sampled blocks, **not** a speedup obtained by
dividing ratios. The raw Paro batch median ratio is 1.242989; the missing second
batch pair and environmental interference prevent the preregistered directional
decision. Warm's observed increase exceeds the registered 10% investigation
threshold, but it is not attributable to the execution change from this run.

## Interference and stop

Ambient VM/background services were disclosed at registration. Source builds
ran serially, outside the query timers, and no active high-CPU compiler was
observed immediately before launching the probe collector. This was not an
isolated-host guarantee.

The probe collector ran approximately 11:14:58–11:15:20 (UTC+08:00). The
11:15:23 postflight snapshot found an unrelated Go linker with five seconds
elapsed, approximately 19% CPU and 9.8% memory, overlapping the collector's
tail. VM activity remained present. Point snapshots do not establish the full
interference timeline or its exact contribution to any individual sample.
DuckDB C1's median also increased substantially, and both engines had a large
third cold sample. No anomalous sample was removed.

Per registration, further batches were stopped, unrelated processes were not
terminated, and the complete collected data were retained. There is no causal
performance, warm non-inferiority, target-pass or parity claim. A future
quiet-host comparison needs a new complete registered cohort; do not splice
these controls into it or selectively repeat the slow probe block.

## Validity checks

- Both source attestations were clean and rebuilt from the selected source;
  saved binaries were not substituted into the collector.
- Both maintained campaign summaries, normal/diagnostic cell payloads and
  typed v3 compile documents passed their shared validators.
- Every cold receipt has a verified cache miss, non-null normal compile work,
  actual grant class 2, completed execution, and a four-task parallelism ceiling.
  No fallback, lowering failure or image failure was observed.
- Q04 returned six rows. Full logical types, multiset and required ORDER
  matched the independent DuckDB oracle in every measured sample. Result
  digest: `1a6b0bddb997ad4cd7bdba5eed155de040c15343ada36153ac8a4a2f40297090`.
- Normal receipts: 69 groups, 77 logical expressions, 121 physical expressions,
  782 cost compositions, two obligations, `QualityPolicySatisfied`,
  `search_complete=false`, `budget_limited=false` in all six cold processes.
- Both independent Detail documents have identical non-time search counters:
  810 proposals, 509 cumulative publications, 782 cost compositions. Their
  observed portfolio/artifact identities match the corresponding normal
  receipts, and each contains the actually admitted fingerprint
  `[5388449204632279338, 1773016664838677076]` at grant class 2. Fingerprints
  locate the plan; correctness comes from the complete typed result checks.
  They do not assert identical execution-machine code across this intervention.
- No implementation, budget, expected SQL output, performance baseline or
  policy was changed this round. The previously completed implementation tests
  remain in [q04-linear-decimal-v1](../q04-linear-decimal-v1/README.md); they
  were not rerun just to archive measurements.

`re-op` and its probe release binary were restored. The owned test listener is
stopped. Existing worktrees, recovery refs, data seeds and unrelated processes
were not modified.

## Reproduction

Use the exact registered clean source per arm, sequential builds and the
maintained collector; never run the two engines' target statements concurrently:

```sh
PYTHONPATH=benchmark PARO_COMPILE_WORK_EVIDENCE=1 \
PARO_DIAGNOSTIC_SEARCH_STOP_MS=30000 \
benchmark/.venv/bin/python benchmark/corpora/tpcds_compare.py \
  --server-data-dir <immutable-relocatable-seed> \
  --duckdb-database <pinned-sf1.duckdb> \
  --dataset-source-dir <pinned-csv-dir> --query-dir <pinned-query-dir> \
  --report <unique-batch-path>.json --start 4 --end 4 \
  --listen 127.0.0.1:16433 --process-blocks 3 --diagnostic-process-blocks 1 \
  --measurement-rounds-per-process 1 --warmups-per-process 1 \
  --bootstrap-samples 10000 --random-seed 2026092405 \
  --threads 4 --memory-limit 2GB --optimizer-search-policy quality \
  --optimizer-verify off --metadata-track generator-declared \
  --paro-result-format binary --build-jobs 4
```

A new collection is not a continuation of this interrupted comparison.
