# Planner cleanup validation

EvidenceId: `planner-cleanup-20260925-v1`

This is a bounded code-cleanup acceptance record, not a competitive-performance
gate or a replacement for the full corpus certification workflow. The baseline
is `85440a138`; each corpus manifest identifies the actual control/probe binary,
dirty source state, SQL, seed, settings and harness revision. The raw logs and
temporary server data are not copied into the repository.

## Correctness

- TPC-H: 22 strict before/after type, multiset and ordered-peer matches; 22 equal
  selected identities and plain EXPLAIN renderings.
- TPC-DS: 98 strict matches; Q39 retains its binary64 difference and has a
  separate independent bounded numerical/relational certificate. Do not count
  it as a strict floating-point match or use a blanket epsilon.
- Five intentional TPC-DS plan differences: Q28/Q32/Q38/Q97 move LIMIT below a
  proven safe projection; Q72 removes an unused unique outer lookup. The other
  94 selected identities and renderings match.
- Fresh-data SQL regress: 185 passed, zero failed/ignored/abnormal. The initial
  block audit and all 24 individually reviewed expected-file hashes are kept
  in `regress/adjudication.json`. No blanket baseline update was run.

`tpcds/` and `tpch/` contain manifests, summaries and per-query result/plan
identities, not duplicate diagnostic traces. They compare the two Paro binaries
on the same fixtures; these are not fresh DuckDB or PostgreSQL oracle runs.
The maintained typed result and compile-document validators are used.

`q39/certificate.json` comes from the existing integer-input/Welford scheduling
oracle, with 360,000 rows, 90,000 groups and 243 output rows. Its per-value
certificate is losslessly column-packed (`numeric_columns` / `numeric_rows`);
the source oracle's `errors` field enumerated observations, not failed checks.
`q39/results.json` preserves both raw engine outputs, and the corpus result
retains the strict mismatches. No candidate-specific tolerance was fitted.

## Performance boundary

`warm/manifest.json` registers the limited pilot and its actual binary hashes.
The probe precedes the final conservative correlated-reference guard; it is
not a timing certification of the final source. The final corpus and regress
runs use the rebuilt delivery binary and are correctness-only runs.

Three fresh blocks, four alternating rounds, eight queries and two arms yield
192 normal samples. Each sample keeps its receipt in its query/arm cell; there
is no Detail timing subtraction, slow-sample deletion or cross-report ratio.
The maintained receipt collector verified all 192 execution associations.
There was external VM/background load, and the pilot has no powered
non-inferiority threshold: status **NotCertified**.

| Query | Control warm median (ms) | Probe warm median (ms) |
| --- | ---: | ---: |
| Q04 | 172.586 | 160.450 |
| Q11 | 92.185 | 89.956 |
| Q28 | 24.185 | 21.356 |
| Q32 | 4.288 | 4.339 |
| Q38 | 90.543 | 83.440 |
| Q72 | 124.637 | 115.754 |
| Q74 | 63.124 | 66.564 |
| Q97 | 57.471 | 56.896 |

These numbers are descriptive only. In particular Q74's slower median is
retained. This record makes no compile/C1, parity or causal speedup claim.

## Reproduction and ownership

Use the versioned `paro-evidence` and `paro-start-regress` workflows against
the selected checkout. The corpus query and seed hashes, source identities and
settings are in the manifests; Q39 references the existing versioned numerical
contract under `benchmark/evidence/optimizer-convergence/20260920/c0/`.
Do not recreate the scratch directory names as shared defaults.

`validation.json` records commands/results and log hashes. `checksums.json`
covers all files except itself, avoiding a self-reference. No free-text server
logs, seed copies, repeated full traces or build products are archived here.
The implementation/disposition and per-case expected rationale are in
[planner-cleanup.md](../../../../../docs/optimizer/planner-cleanup.md).
