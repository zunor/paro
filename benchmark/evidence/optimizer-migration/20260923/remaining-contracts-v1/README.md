# Remaining historical contracts: matrix result

Registration: [remaining-contracts-registration.md](../remaining-contracts-registration.md).
Code: `03937e99` and `5106bffe`, from clean `re-op` base `bf1fc03f`.
Covered collection source: `256e330f`; release SHA-256
`1babf012f98913d01d4617e39f0d9803e3a976998c96e9ccfd4c5df1060e5285`.

## Delivery boundary

Two contracts are migrated:

1. Initial SQL normalization and native staging coalesce adjacent total,
   schema-preserving restrictions. Fallible/volatile evaluation, reordered or
   narrowed layouts, facets, context changes and unavailable facts remain
   barriers. Rebinding an input does not retag a frozen resident contract.
   Existing output-relation estimates remain owned by that relation.
2. Native join atoms read authoritative, observed Memo cardinality. Missing
   evidence uses the configured ranking prior, not a fabricated one-row fact;
   unknown provenance survives DP composition and is not published as exact
   cardinality. Local materialization-risk witnesses remain distinct.

The third proposed port, deferred extraction of unselected physical variants,
is **not delivered**. The old binder closure would bypass current pre-admission
physical verification, complete dependency inventory and typed identity.
Execution-image lowering is already lazy but is a different boundary. A future
implementation needs a typed immutable extraction recipe, separate recipe and
materialized-plan identities, complete pre-admission dependencies, and real
fallback/failure/cancellation tests. No callback compatibility layer, skipped
verification or removed grant is shipped to manufacture a compiler reduction.

## Normal compiler and end-to-end results

`work-*` runs enable the existing bounded normal compile-work receipt observer.
They remain trace-off, fresh-process and target-cache-miss measurements. Detail
has a separate process and is not a compiler/C1 timing source. All values below
are milliseconds; five off blocks, two on blocks per query.

| Query / verifier | Compiler median / P90 | Paro C1 | DuckDB 1.5.5 C1 | Paro warm | Syntheses |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 / off | 19.150 / 23.119 | 211.276 | 106.391 | 129.951 | 810 |
| Q11 / off | 12.510 / 15.173 | 131.674 | 53.541 | 76.896 | 420 |
| Q74 / off | 65.957 / 81.705 | 192.486 | 97.606 | 60.179 | 1,065 |
| Q04 / on | 23.914 / 27.648 | 267.027 | 186.837 | 162.930 | 810 |
| Q11 / on | 13.818 / 14.073 | 167.883 | 144.762 | 84.908 | 420 |
| Q74 / on | 87.793 / 90.557 | 259.849 | 115.565 | 74.061 | 1,065 |

All normal compiler samples, in collection order:

- Q04/off: 19.150, 23.119, 19.278, 18.551, 18.854.
- Q11/off: 12.108, 12.510, 12.434, 13.628, 15.173.
- Q74/off: 64.600, 64.131, 65.957, 81.705, 74.481.
- Q04/on: 27.648, 20.179.
- Q11/on: 13.563, 14.073.
- Q74/on: 85.029, 90.557.

P90 uses the existing collector's nearest-index convention (with these small
sample counts it is the maximum). No slow value is removed. Background system
activity and substantial DuckDB drift make these **NotCertified pilots**;
on/off is not a paired estimate of verifier overhead. Generator-declared
metadata is also not a symmetric first-statement parity gate.

Q11 retains the recovered approximately-12-ms scale, not a strict <=12.000-ms
or <10-ms result. No new causal speedup is established: synthesis counts and
selected typed structural locators match the prior cardinality-owner runs.
The per-query normal counts are stable across all samples:

| Query | Groups | Logical | Physical |
| --- | ---: | ---: | ---: |
| Q04 | 69 | 77 | 121 |
| Q11 | 53 | 61 | 105 |
| Q74 | 95 | 136 | 217 |

All stop with **QualityPolicySatisfied + SearchIncomplete**, not ProofComplete.
Actual admission is class 2. Normal receipts and the corresponding diagnostic
capture have matching artifact identities and stop states. This permits their
bounded joint interpretation; a matching fingerprint alone is not a SQL or
plan-equivalence proof. Result validation is independent and complete.

## What the compile evidence supports next

Q74 remains the principal held-out optimizer problem: 1,065 syntheses, 466
recipe processings, 486 physical ReadSet rebuilds and 51 quality evaluations
in its separate off Detail capture. Complete aggregate evidence appears at
36.204 ms, while full join/aggregate/domain coverage appears at 64.085 ms.
These diagnostic milestones do not prove queueing is the cause. Rule totals
alone do not account for its optimizer wall time. The next experiment should
attribute necessary physical/quality work on that exact candidate path, not
add another general cache, reduce the search budget, or restore provenance-
based quality certification.

Detail is bounded and intentionally incomplete: Q11 omits 114 capture and
1,391 encoding records; Q74 omits 1,884 capture and 1,391 encoding records in
the off captures. Summary/counters remain available; omitted per-event data
must not be reconstructed or described as a complete execution history.

## Collection correction and validation

The first six `q*` runs omitted the normal compile-work observer. Their C1,
SQL and receipt evidence remains valid, but compiler timing is **Uncovered**.
They are retained verbatim and are not pooled with `work-*`. The correction was
registered before replacement collection. No source/binary intervention,
diagnostic subtraction, retrospective timer fill or retry-until-fast was used.

- Every measured query passed complete type, bag and required-order checks:
  Q04 6 rows, Q11 90 rows, Q74 92 rows.
- Maintained campaign, payload/receipt and Compile document validators pass
  all twelve retained runs. Compiler coverage is only claimed for `work-*`.
- Final `make test`: 6,913 passed, zero failed, 85 ignored; optimizer 1,375
  passed. Strict workspace Clippy, benchmark's 206 tests, release build,
  memory/vector API guards and calibration-artifact check pass.
- High-FD verifier-on SQL regress: **167 passed / 18 failed**. Failure-file set
  and all eighteen actual outputs are byte-identical to the pre-task control.
  No expected output was changed; this is not an all-green SQL gate.
- Skill preflight now names the normal compile-work observer explicitly.
  Its external quick validator lacked PyYAML in the benchmark venv; frontmatter
  was unchanged and the small Markdown-only edit was reviewed directly.
- A final workspace rerun exposed an unrelated render-test lifetime race:
  fixtures held finished captures while reserving another, competing for the
  real process limit of eight. Tests now release the first capture once its
  independent JSON bytes exist. Production capacity and code are unchanged;
  seven concurrent render tests and the final full workspace rerun pass. The
  failing log is retained, not reclassified as a pass. Repository-wide
  header checking still reports 169 pre-existing violations outside this task;
  no all-static pass is claimed.

Bounded RunOutput directories retain inputs, ownership, receipts, all samples
and one capture per cell (about 3.3 MiB total). Raw logs and full regress outputs
remain at `/private/tmp/paro-historical-contracts.EPuI9D`, outside Git. No old
worktree, recovery reference, dataset or user file was deleted.
