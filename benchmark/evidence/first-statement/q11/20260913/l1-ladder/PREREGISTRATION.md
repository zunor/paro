# L1-LADDER — registered measurement-only protocol

Reviewed and registered by main 2026-09-13; **not executed or syntax-tested during
M-POWER**. Main has read the driver and protocol; runtime validation follows
M-POWER completion. No
engine, matcher, cost model, benchmark-platform, or instrumentation change.
Writes in preparation are confined to this evidence directory. No concurrent
build/test/Paro/DuckDB/query/benchmark processes may be started by this task.

## Question and scope

Locate which small workload branches reproduce the observed second-execution
Paro/DuckDB gap, while checking actual cache reuse and zero buffer fills in each
measured process. This is a workload decomposition, not a causal attribution of
Q11 wall time. A fresh process is not a cold OS cache. Seed hashing and private
copies themselves touch the filesystem; we do not flush caches or claim disk-cold
measurements. No CPU profiles, new counters, or new instrumentation.

The artifact imports `tpcds_compare.py`, `benchmark_evidence.py`, and
`tpcds_result_contract.py` from a supplied **clean runtime repository**, never
from the surrounding dirty development workspace. It reuses
`isolated_paro_server` / `ManagedParoServer`, `ImmutableDataSeed`,
`DuckDBProcess`, Paro configuration, metadata inventory/validation, the actual
occurrence cache helper, typed full-result comparison, and existing statistical
and report helpers. It does not call `build_benchmark_server` or Cargo.

## Frozen rungs

| Rung | Exact workload (aliases fixed by driver) | Interpretation limit |
| --- | --- | --- |
| L0 | `count(*) FROM store_sales` | May be metadata-only on either engine; not a scan-throughput test. |
| L1 | `count(ss_customer_sk) FROM store_sales` | Nullable-key count; verify plan before claiming column consumption. |
| L2 | `sum(ss_ext_list_price) FROM store_sales` | One price-column reduction. |
| L3 | `sum(ss_ext_list_price - ss_ext_discount_amt) FROM store_sales` | Price-minus-discount expression and reduction. |
| L4 | `count(*) FROM store_sales WHERE ss_sold_date_sk BETWEEN a AND b` | Independent date-predicate/count branch, not L3 plus a filter. |
| L5 | `ss_customer_sk, sum(ss_ext_list_price - ss_ext_discount_amt) FROM store_sales GROUP BY ss_customer_sk` | Original requested grouping, **no ORDER BY**, no LIMIT. |
| L6 | `count(c_email_address) FROM customer` | Separate table/string-null-count branch; may not consume string payload bytes. |
| L7 | Existing `e2-decouple/pre-touch.sql`, byte-for-byte | Four checksum rows consuming the original Q11-referenced columns; not full Q11. |

L7 uses the existing E2 SQL **as the target**, not as a pre-touch before another
rung. It retains the casts/COALESCE/UNION ALL in that file. No new joins, forced
scans, predicates, result casts, or ORDER BY clauses are inserted to equalize
plans. L5 validation is an exact unordered typed bag, including NULL keys and
duplicate row multiplicities; order is not validated or timed separately.

Only when L4 is selected, a separate preparation `DuckDBProcess` opens the
reference database read-only and obtains min/max `d_date_sk` for
`d_year BETWEEN 2001 AND 2002`. It verifies non-null, unique, contiguous keys and
zero symmetric-difference rows between the year and key-range predicates.
The actual integer literals are substituted into **one identical SQL string for
both engines**. Archive the derivation/check SQL, results, process identity and
reference database hash. Never run independent engine-specific bound derivation
or silently add a date join. Preparation is outside every measured process.

## Sampling and eligibility

- Primary cohort: **36 independent fresh process pairs per rung**, no CLI N
  override. Each pair has one new Paro process on its own immutable-seed snapshot
  and one new read-only DuckDB process. Each target executes exactly twice per
  engine and every result is fully fetched. No target warmups or retries.
- A=Paro, B=DuckDB. Precompute 18 ABBA and 18 BAAB blocks, shuffled with seed 1
  (archived if changed before sampling). ABBA means P0,D0,D1,P1; BAAB means
  D0,P0,P1,D1. Both processes are ready and metadata-checked before the four
  calls; startup order is always Paro then DuckDB. Calls are serial.
- C1 is actual target occurrence 0, W is actual occurrence 1. Do not call both
  executions "warm" or give either engine extra repetitions. C1 and W have
  different within-block positions; balanced order does not remove all history
  effects. No concurrent other workloads, tests, compiles, or database users.
- Runtime: four workers, 2GB, Paro binary result format, optimizer verification
  on, existing handoff on, compile-work evidence on, model/budgets unchanged.
  Metadata track is required on the CLI; Paro and DuckDB key inventories must
  agree and the selected Paro track must validate. Metadata SELECTs/configuration
  precede the target but never execute its SQL or scan its user-table contents.
- Every Paro target is followed immediately by the existing post-timer cache
  lookup, using explicit `expected_occurrence=0/1`. Require verified first miss
  and second hit for the exact SQL fingerprint. Each record must have valid
  isolated E1 metrics, no operator overflow, and an actual execution ID. W must
  have a later execution ID, the same image ID, `buffer_fill_count=0` and
  `buffer_fill_input_bytes=0`. Missing evidence is not zero. Compile-work and
  remaining E1 metrics are archived, not converted to guessed missing values.
- All four full result sets pass the existing typed schema and exact multiset
  checks, both between engines and between occurrences. Retain row counts,
  full-bag digests, schemas and individual execute/fetch timings. Validation and
  evidence reads are outside the timers but part of the inter-call preparation
  history. No floating tolerance, checksum-only substitute, row sampling, or
  dropping duplicates to make equality pass.
- Any execution, type, bag, cache, zero-fill, metadata, or identity failure stops
  the campaign. Preserve its partial record/logs; no replacement blocks, skipped
  slow observations, continuation under a new label, or significance stopping.

## Instrument cohorts and timing interpretation

Per-occurrence fill-zero proof requires `PARO_COLD_WORK_EVIDENCE=1`. Therefore
the primary driver is **E1-on / statement-trace-off**, not normal E1-off timing.
Existing E1 includes worker accounting and occupancy synchronization; its
overhead is not established negligible, and may differ sharply by rung. Do not
pool these samples with M-POWER normal timings, certify normal parity, or use a
worker-nanosecond sum as critical-path wall time. Inherited `PARO_*` switches and
`RUST_LOG` are cleared, then the four declared cache/cold/compile/handoff flags
are set; all overrides are archived. No stream/pruning/strong-incumbent variant.

`--cohort diagnostic` is an explicitly separate **one-pair** inspection, with
statement trace and E1 enabled. It also executes the target exactly twice and
validates its bags/cache/fills, but emits no primary ratio summary. Optional
`--capture-plan` runs plain EXPLAIN in each engine only **after** both samples
and evidence reads; it is not EXPLAIN ANALYZE or a third target execution. The
existing statement traces and plan text can determine whether L0 avoided a scan.
No diagnostic timing enters the 36-block cohort. An optional future E1-off
overhead comparison must be separately registered; it cannot retrospectively
supply fill-zero proof for its individual samples.

The timer is the existing per-engine execute + full fetch + native result
metadata interval. DuckDB IPC delivery to the parent and canonical Python type
conversion are outside its timer; Paro includes its wire/client fetch path.
For tiny L0 this asymmetry/fixed client overhead can dominate. A ratio of four
therefore does not locate a scan bottleneck, even if repeated precisely.

## L0 pilot gate before any later rung

First run L0 alone for all 36 pairs. The driver refuses L0 mixed with later
rungs and always returns for review afterwards. There is no automatic ladder
continuation and no mid-pilot stopping on a timing value.

**Registered operational trigger:** geometric mean of the 36 raw W P/D ratios
>= 4. Pass the explicit `--pilot-stop-ratio 4`; other thresholds are outside this
registration. The driver uses the unrounded estimate. Report the confidence
interval too; this operational point-estimate gate is not a significance test.

When the trigger fires, stop later rungs pending examination of the actual L0
plans, result size, timer path and evidence. A large metadata-only ratio is not
evidence for foundational scan throughput. Continuing after a trigger requires
a reviewed record stating `foundation_causal: false`, a concrete reason, and
hashed existing diagnostic evidence. That decision does not prove the cause or
license forcing L0 to scan. If foundational attribution remains unresolved,
stop and report underidentification rather than running the remaining rungs.

Any later rung requires a complete eligible L0 report from identical
source/build/harness/data/metadata/configuration and a review JSON, for example:

```json
{
  "pilot_report_sha256": "<sha256 of the completed L0 report.json>",
  "decision": "continue",
  "reviewer": "<main reviewer>",
  "reason": "<recorded decision, not inferred automatically>",
  "foundation_causal": false,
  "evidence": [
    {"path": "/absolute/path/to/reviewed-diagnostic/report.json", "sha256": "<sha256>"}
  ]
}
```

For a non-triggered pilot the foundation field/evidence are optional, but an
explicit review is still required. Never fill out this review before seeing
the pilot. Keep it outside the clean runtime repository.

## Analysis and identification limits

Primary estimand per rung: geometric mean of the 36 paired W P/D ratios. Report
C1 separately, all raw process observations, medians and p95 by engine, and a
95% whole-process-pair bootstrap interval (10,000 resamples; existing helper).
Each block contributes one C1 pair and one W pair to their **separate** analyses.
The helper named `hierarchical_cold_ratio` is reused for its one-pair-per-process
resampling algorithm; the W adapter is labeled occurrence_1 in this report.
Do not use `hierarchical_abba_ratio` to mix C1 and W, or resample them as two IID
warm observations. Report order sensitivity if the orders disagree; N=36 is the
requested fixed conditional baseline, not a proven power guarantee for every
rung or evidence that the process tails are stationary.

L0→L1→L2→L3 are workload contrasts, not pure additions of operators: null
semantics, metadata shortcuts, row widths and plans can change. L4, L5 and L6
are **branches**, not successive stages; summing their differences is invalid.
L5 changes result cardinality/client transfer and grouping, L6 changes table
and operation. L7−L3 is a catch-all contrast spanning extra tables, arithmetic,
strings, checksum casts and execution setup; it cannot establish that any one
component accounts for 50% of Q11 or of the ladder gap. Telescoping endpoint
differences is an identity, not identification. If multiple explanations fit
the observations, report underidentification. There is no new platform,
mechanism implementation, or global-priority change in this task.

## Reproducibility, execution command, and handoff

After M-POWER, main builds the latest reviewed committed source once in a clean
runtime repository and records the exact clean-source build attestation produced
by `build_benchmark_server` (or a report containing it). Use that executable for
the entire ladder. Do not use c23ae52a: it lacks the required E2 SQL and has not
established compatibility with the second-occurrence evidence contract. The
runtime must include the existing E2 SQL and corrected cache/E1 helpers; no
compatibility workaround or engine change is part of this artifact. Commit the
driver/protocol separately. `--expected-commit` identifies the chosen clean
runtime, not the surrounding development workspace's HEAD.
Check full expected commit, clean status and binary SHA-256; record engine/build
input digest, all corpus Python helper hashes, driver/prereg hashes, DuckDB
extension/version/database hash, immutable Paro seed hash, dataset source hash,
metadata inventories and exact SQL bytes/hashes. Recheck identities after the
campaign. Private outputs must be a new directory outside both repositories
and input data. The driver performs no build. Do not mutate seeds or run their servers
in place. The normal helper timeout applies to Paro; DuckDBProcess retains its
existing blocking worker protocol, so an externally interrupted hang invalidates
the incomplete campaign rather than authorizing replacement samples.

**L0 pilot command — main runs serially after M-POWER and runtime validation:**

```sh
/path/to/clean-runtime/benchmark/.venv/bin/python \
  /Users/linjunhong/workspace/paro/benchmark/evidence/first-statement/q11/20260913/l1-ladder/measure.py \
  --runtime-repo /path/to/clean-runtime \
  --expected-commit <full-reviewed-commit> \
  --binary /path/to/attested/parod \
  --build-attestation /path/to/existing-build-attestation-or-report.json \
  --server-data-dir /path/to/offline-paro-seed \
  --duckdb-database /path/to/reference.duckdb \
  --dataset-source-dir /path/to/tpcds-source \
  --metadata-track generator-declared \
  --listen 127.0.0.1:6432 --rungs L0 --pilot-stop-ratio 4 \
  --output-dir /tmp/paro-l1-ladder-L0-scalar36
```

Resolve placeholder paths and the actual metadata track before execution; the
registered trigger is 4. Use an isolated unused listen address. This command
runs only L0. A separate plan diagnostic uses the same
arguments with `--cohort diagnostic --capture-plan` and a new output directory.
After pilot review, replace `--rungs L0` with the selected later rung(s), supply
`--pilot-report` and `--pilot-review`, and use another new output directory.
Every selected primary rung still has exactly 36 fresh pairs.

After M-POWER stops, main commits these reviewed artifacts, builds the clean
runtime once, checks helper compatibility, and runs syntax/runtime validation
before sampling. Preparation here has run **no driver, imports, SQL, build, test,
query, benchmark, DuckDB process or profiling command**. No claims of runnable
validation or observed ladder performance are made in this registration.
