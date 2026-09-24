# Q04 scan/aggregate internals and placement validation

EvidenceId: `q04-execution-internals-20260924-v1`.

Status: implementation and result checks completed; performance **NotCertified**.
This pilot does not certify an isolated two-stage-aggregation benefit, C1 parity,
or complete search. Read [registration](registration.md) and the preserved
[placement negative result](placement-outcome.md) before interpreting timings.

## Implementation

- `17f70b01`: selected string/dictionary decode consumes the validated row
  selection before assembling output vectors. Plain strings retain an immutable
  page owner; dictionary output reuses its immutable child and survivor codes.
  NULLs, duplicate/nonmonotonic selections, whole-batch corruption checks and
  fallible copy-on-write remain enforced. This does **not** claim that LZ4 can
  decompress only selected rows; compressed pages remain the decode unit.
- The same commit adds a bounded ARM64 NEON inverse bit transpose for 32/64-bit
  numeric pages, with portable/tail paths and an independent scalar oracle.
  The persisted BSH2 format does not change.
- Aggregate fixed keys now use the existing prepared batch views, including the
  adaptive integer index. Variable-key budget preparation walks only variable
  columns instead of doing empty per-row work on fixed-key batches. Hash/equality,
  NULL masks and memory/spill contracts do not change.
- `782fc402`: grouped primitive SUM resolves the flat input and existing state
  cursor once per batch. It preserves row order, floating-point additions and
  integer overflow behavior; no reassociation or fast-math is introduced.
  Constant/dictionary inputs retain their correct generic path. The same kernel
  serves finalized partial-SUM reducers; no final aggregate is removed.

These are reusable execution contracts, not Q04-specific operators or switches.
The only new experiment control exposes the existing public disabled-rule
setting in the maintained collector and records the changed search domain.

## Final normal observation

Final source: clean `5251cfd4d60e18d5853bc490c1316e7398088cd0` (code through
`782fc402`). Release binary SHA-256:
`5092c7c1d6521b39392deddae645e7c70264d588346e8e8857f5a6b6c2e577ed`.

Two fresh process blocks, three warm ABBA rounds per block, DOP 4, 2 GB,
DuckDB 1.5.5, normal trace off, bounded compile-work receipts on. The immutable
dataset/SQL/runtime identities are in each RunOutput `inputs.json`. No owned
build, test or other benchmark overlapped collection. Ambient VM/database
activity remained; metadata is generator-declared in Paro. These limitations and
the small sample were declared before collection, not excluded afterwards.

| Metric | Paro | DuckDB |
| --- | ---: | ---: |
| C1 median | 219.611 ms | 144.978 ms |
| C1 samples | 214.672, 224.550 ms | 114.565, 175.390 ms |
| Warm median (12 samples) | 128.509 ms | 115.154 ms |

The producer's paired C1 ratio is 1.548874, bootstrap interval
[1.280293, 1.873797]; this two-block interval is descriptive, not certification.
Cold receipt compiler times are 19.094 and 18.599 ms, each with 782 cost
syntheses. Both admit the same class-2 physical locator
`[5388449204632279338,1773016664838677076]`, with identical resource contracts.
Stop is `QualityPolicySatisfied`, `search_complete=false`; not `ProofComplete`.

Every normal sample passed complete six-row type/bag/ORDER validation. The shared
validator accepts all normal per-sample compile/admission/execution receipts.
The separate COMPILE-only diagnostic has no execution receipt by design; it is
validated as a compile document, not mislabeled as a selected execution.

No same-batch, same-policy old-binary control was collected for the implementation
patches. The preceding reference cohort is retained but **not** a causal speedup
claim. In particular, there is no demonstrated independent SUM-kernel C1 benefit.

## Does Q04 benefit from preaggregation?

The completed rule-enabled/rule-disabled budgeted cohorts selected the **same**
single-stage physical candidate. The intended stage contrast was not obtained;
further identical cohorts were stopped and labeled NotCollected. Both retained
cohorts hit the declared 30-second optional deadline (about 33-second C1 including
the tail). This is the experimental budgeted policy, not the quality-policy C1
above, and not evidence of complete optimal search.

The separate quality candidate has three narrow partial aggregates before the
customer joins and three final merges. ANALYZE establishes these row counts:

| Branch | Partial input | Partial output | Final merge output |
| --- | ---: | ---: | ---: |
| Store | 1,096,053 | 76,100 | 76,098 |
| Catalog | 572,007 | 54,461 | 54,459 |
| Web | 289,524 | 22,806 | 22,804 |

Thus narrow aggregation reduces 1,957,584 rows to 153,367 before the customer
joins. This is real work reduction, **not** an isolated elapsed-time proof. The
single-stage candidate has different join placement, an additional RF, repeated
date filters and different CTE filters. Its wide aggregates consume 1,930,237
rows and emit the same 153,361 final rows. Instrumented operator durations are
neither additive wall time nor normal C1.

Two concrete remaining optimizer issues must not be hidden by operator counts:

1. The budgeted candidate repeatedly applies the same two-year predicate along
   the date scan path. Estimated rows repeatedly shrink (721, 541, 406, 305, ...)
   even though the predicate is already enforced. Guaranteed-domain transfer,
   predicate idempotence and costing need one contract; simply increasing search
   selects more incorrectly discounted alternatives.
2. `selected_aggregate_region_witnesses_refs` currently derives coverage from
   `shape.decomposed`; `aggregate_merge_contract_matches` proves the legal merge
   structure. This is not a profitability proof. This patch does not change that
   policy or certify it as one. A future placement decision must compare legal
   alternatives in the same fact/resource/parent-response context, including
   input-expression work, key width, group count and final-merge cost, with a
   controlled exact-plan replay. Near-1:1 observed final merge rows are not a
   uniqueness proof and do not authorize removing that operator.

## Validation and retained evidence

- `RUST_MIN_STACK=33554432 cargo test --workspace --locked -j 4 --quiet`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- Release build: passed. Benchmark suite: 229 passed (one pre-existing pytest
  return-value warning, not a result failure).
- Storage: 2,096 passed / 1 ignored; execution: 674 passed; function: 591 passed;
  optimizer: 1,407 passed. Primitive SUM subset: 12 passed.
- Memory/runtime and fallible vector-copy guards: passed. The repository-wide
  header check reports 169 pre-existing issues in 57 unchanged files; it is not
  reported as a passing gate or repaired in this execution task.
- Full SQL compare-only regression: **185 passed, 0 failed, 0 skipped, 0 new**
  in 75.63 seconds. Final release binary, fresh owned data, four workers/2 GB,
  FD limit 65,536; `PARO_UPDATE=0 PARO_WRITE_ACTUAL=1 make -C regress ci` on
  owned port 16434. No expected files changed.

`matrix/` contains the four maintained RunOutput campaigns, all samples,
per-sample receipts and one bounded compile capture per diagnostic cell. The
supplementary ANALYZE archives retain structure/cardinality/result evidence, not
raw event floods. The incomplete first single-stage diagnostic remains present;
its replacement and exact limitations are explained in `placement-outcome.md`.
The supplementary reproduction script's cumulative UTF-8 output guard was
tightened after collection; this does not retroactively certify an enforced
stream cap for the original captures, which remain unchanged.
No baseline, policy, search budget, SQL expected result or historical archive was
blessed or deleted.
