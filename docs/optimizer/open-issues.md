# Remaining performance and oracle work

Prioritize new measurements, not old medians. Routine investigation uses
`paro-benchmark`; only a formal release/parity claim uses `paro-evidence`.

- **Window execution:** rank current corpus excess time, then profile sort,
  partition discovery, input evaluation and result production. SQL categories
  alone do not establish a cause.
- **First execution:** measure metadata/segment opening, buffer fill/decode,
  admission/image construction and scheduling directly. Subtracting cold and
  warm medians is not a measured decomposition.
- **Dynamic range filtering:** verify build-key bounds reach eligible scans
  and can safely prune blocks; preserve NULL, outer/anti and snapshot semantics.
- **Q74 follow-up:** the last eight-query warm pilot at `769ad6104` had probe/
  control median ratio about 1.055 amid external load. Recheck a balanced quiet
  pilot before attributing regression; don't discard this query from rankings.
- **Scan order:** segment order is an optional physical choice, not pruning or
  early LIMIT. Upper TopN owns LIMIT/OFFSET. Test concurrent publication,
  deleted versions, unknown bounds, and a visible maximum outside the preferred
  first segment (including the bulk-delete maximum-id 19999 regression).
- **Shared-CTE RF:** prove the complete producer/consumer ownership and domain
  coverage across all eligible references; never follow one CTERef and infer
  safety for all. Test different local predicates, recursive/nested boundaries
  and outer-join preserved sides.

## Reproducible correctness limitations

**TPC-DS Q39 binary64:** strict engine comparison can differ in low bits.
Regenerate the declared SF1 input using the maintained corpus tools, run Q39
with strict results retained, and capture its exact input bags using
[`corpora/contracts/q39`](../../benchmark/corpora/contracts/q39/README.md).
The independent bounded integer/Welford contract certifies relational keys,
order and the admitted numerical schedules, not arbitrary float tolerance.
Changed input outside that domain is Uncovered. Do not convert this exception
into a global epsilon or claim every result is bitwise identical.

**TPC-H checked-in oracle applicability:** the historical identical-input
DuckDB audit also failed Q01/Q02/Q10/Q13/Q15/Q20 against the checked-in expected
oracle. Five ordered digests matched Paro; Q01 involved exact floating results.
Reproduce with `benchmark/workloads/tpch`, the same dbgen scale/seed and normalized
trailing-delimiter input, then compare both engines' complete typed output before
adjudicating an expected change. The original dataset/harness hashes and audit
are at `769ad6104:benchmark/evidence/optimizer/20260925/relation-convergence-v1/`.
Before/after 22/22 equality does not close this independent-oracle limitation.
