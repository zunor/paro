# Aggregate representation and ownership

EvidenceId: aggregate-contracts-v1-20260924. Collection was registered in
[registration.md](registration.md). Sampled source: `f3e92fbf`; implementation:
`0c58eb3d`, `57a98fe2`, `09322560`. Each RunOutput records its build and inputs.

## Implemented contracts

1. Retained aggregate keys can use the existing packed-string/integer-offset
   codecs even when functional-dependency reduction maps dependent columns to
   aggregate states. One emit projection restores keys and maps finalized
   states directly to SQL output, for both in-memory and spilled results.
   Paro already had these key codecs; this removes their incompatibility with
   dependent-state projection, rather than introducing a second representation.
2. Equality alternatives on one integral column are disjoint events.
   Selectivity sums distinct coordinates instead of applying independent-OR
   probability. Repeated `year=2001 OR year=2002` over estimated NDV two no
   longer discounts rows by 0.75 per wrapper. This remains an estimate, not a
   guaranteed domain fact or permission to remove a predicate.
3. An empty merge destination can adopt its first non-empty owned fragment
   under the same accounting target and update contract. Tuple data, index,
   key heap, aggregate arena and allocation leases move together. Remaining
   fragments use the existing serialized merge. Choosing the largest source
   was deliberately rejected because it would reorder floating-point combines.
   Different accounting targets retain the ordinary accounted merge path.

## Exploratory results

Two fresh processes per query, with a separate diagnostic cohort. Every target
normal sample was trace-off and a cache miss. Complete types, multiplicities
and required ordering matched DuckDB: Q04 six rows, Q11 90, Q74 92.

| Query | Paro C1 median ms | DuckDB C1 median ms | Paro warm median ms | Cold compiler ms, both samples | Syntheses per cold compile |
| --- | ---: | ---: | ---: | --- | ---: |
| Q04 | 260.186 | 154.892 | 146.558 | 20.206, 21.377 | 543 |
| Q11 | 155.443 | 77.710 | 89.140 | 13.700, 12.571 | 380 |
| Q74 | 211.731 | 129.685 | 62.187 | 113.986, 36.002 | 1052 |

All six normal compile receipts report `QualityPolicySatisfied`,
`search_complete=false`, `budget_limited=false`. They are not ProofComplete.
Compiler numbers come from normal receipts, not by subtracting diagnostic
times from C1. The three Summary captures are retained once each.

This is a single candidate arm on a host with other user workloads, not a
matched before/after experiment. No causal speedup, non-inferiority or parity
claim is supported. In particular Q74's slow compiler sample is retained, not
excluded. The costing correction may change work and choices, so these are
not a same-plan measurement of the execution changes alone.

## Validation

- Final source: `RUST_MIN_STACK=8388608 cargo test --workspace --locked`
  passed, including integration tests and the existing ignored doc tests.
- Final execution suite: 679 unit tests plus memory/ownership guards passed.
- Cost-model suite: 42 tests passed; extraction/codec/owned-merge targeted
  tests cover packed keys plus state projection, NULL/string handling,
  ownership transfer, incompatible state functions, accounting targets,
  exact-once destruction and floating-point fragment order.
- Strict workspace Clippy, memory runtime and vector-copy API guards passed.
- Final release SQL regress: **185 passed, zero failed, zero skipped**; no
  expected outputs changed. See [regress-report.txt](regress-report.txt).
- Maintained campaign, benchmark receipt and compile-document validators
  accepted all three archived RunOutputs. Their combined size is 820,850 bytes,
  below the registered 16 MiB budget. No raw server logs are archived.

## Remaining aggregate placement work

The cost-based placement task is **not complete**. In particular,
`selected_aggregate_region_witnesses_refs` still uses `shape.decomposed` for
coverage. A legal partial/join/final merge contract proves semantics, not that
this placement is preferable to a single aggregate. This turn did not change
the quality policy, optional budgets or early-stop policy.

The next implementation must compare legal placements under an explicit local
scope: identical output contract, facts/statistics, grant, requirements and
parent source-response context. Evidence must originate from actual current
pricing (including rejected alternatives), subscribe to its dependencies and
expire with them. Winner archive entries alone are insufficient: they retain
historical costs after facts change, and a parent runtime filter can reverse
child scalar ordering. A frontier may also discard the alternative whose
evaluation is needed to attest coverage. Neither scanning that archive nor
counting aggregates is a valid replacement.

A local coverage certificate must remain distinct from global search
completion and must admit a cheaper single-stage plan. Independent tests need
both single-stage and split winners, stale-fact invalidation, parent-RF order
reversal, and NULL/duplicate/outer-join legality. Q04 still needs a controlled
placement comparison before claiming that its two-stage shape is beneficial.
