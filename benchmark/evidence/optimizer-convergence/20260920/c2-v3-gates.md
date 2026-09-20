# C2 result-contract continuation: NOT CLOSED

This is a correctness record, not a performance campaign. No optimizer stopping
policy, search budget, cost coefficient, chain/handoff default or executor was
changed. F2 remains independently unadmitted. Neither an execution-success flag
nor equal failures in two arms is a correctness certificate.

## Implemented contracts

- `b2fca07f`: exact coefficient/exponent numeric identity, independent of Decimal
  precision; no lossy normalization, float conversion or boolean/integer collapse.
- `d6575fb5`, `93720834`: one bound SQL output contract for identity, own wire
  types, scoped integer mappings, exact bags and semantic ORDER keys. See
  [contract v3](typed-result-v3.md). Digests are evidence identities only.
- `fae2d105`: eight individually verified lineage-only expected updates, with
  ID presence/alias/role counterexamples; see [adjudication](c2-lineage-snapshots.md).
- `fec02b59`: RESET restores the observed startup memory envelope, not a
  hard-coded server default; see [resource contract](c2-resource-setting-test.md).

## Both immutable 99-query captures, same v3 protocol

[Full ledger](c2-v3-corpus-gate.json) contains all six verdict dimensions for
every result set, original capture SHA, comparator hashes, pinned catalog and
source/oracle manifest. Target queries were **not rerun**. The original 198
captures, v2 errors and numeric certificates were not overwritten.

| Arm | Exact certified | Independently bounded | Uncovered | Other failure |
| --- | ---: | ---: | ---: | ---: |
| control | 95 | 1 | 3 | 0 |
| probe | 95 | 1 | 3 | 0 |

The numerical certificate joins by capture/input/spec hash, not by query ID.
Q39 retains 57/48 raw exact-row discrepancies and exact ORDER-key disagreement;
the existing independent relation/schedule certificate supplies its bounded
certification. It is not a new approximate comparison rule.

### Remaining ORDER contract: exact minus approximate

Q47, Q57 and Q89 pass identity, type, own-wire/value and exact selected bag
checks on both arms. Their first ORDER key is `sum_sales - avg_monthly_sales`,
mixing exact Decimal SUM and binary64 window AVG. Q57 additionally specifies
NULLS FIRST; the other two use the declared default. All have LIMIT 100 and
additional tie keys. Their row sets agree, but that alone does not certify
ordering or the LIMIT boundary.

Minimal unsupported shape:

```sql
SELECT exact_sum, approximate_mean
FROM input
ORDER BY exact_sum - approximate_mean NULLS FIRST
LIMIT 2;
```

The remaining work is an explicit typed SQL coercion/rounding contract for this
mixed ORDER expression, including peer and overflow boundaries. Converting all
exact values to float is not a solution: the counterexample `10^38-1 + 0.0`
must not silently erase exact identity. Neither a generic epsilon nor Decimal
context expansion is introduced. These three remain **Uncovered**, not renamed
passes. The earlier developmental d6575fb5 replay reported 98 exact; it allowed
implicit mixed floating coercion and is **superseded**, not final certification.

## SQL regress: 27 original failures adjudicated, 18 remain open

Full verifier-on run on clean `fec02b59`: **166 passed / 18 failed**, 74.15 s,
FD 65536, serial server, four threads, 2 GB. The binary is unchanged from the
attested production probe in `c2-source-manifest.json`; changes since that build
are tests, comparison tools and documents. Eight lineage cases and the memory
case now pass in the actual full suite, not just offline text comparison.

[Every remaining differing block](c2-v3-regress-blocks-single.json) records old
expected and new actual hashes, SQL and all mismatching block numbers. This is
a **single-run expected comparison**, not a new paired experiment. Original
27-case captures remain archived. No expected file outside the nine expressly
adjudicated cases was updated.

| Remaining case(s) | Open decision / why not blessed |
| --- | --- |
| fulltext_exec_mode_split, fulltext_index_coverage_guard, fulltext_pg | TopK provider replaced by filter plus full ordering. Exec-mode test explicitly requires TopK; same rows do not exercise that contract. |
| fulltext_rank_cd | TopN versus full sort/LIMIT requires plan-contract review. |
| agg_join_subsumption | Cardinality, RF builder count and artifact identity change together; not a display-only update. |
| agg_singleton_groups | Partial aggregate/outer-join structure changed; singleton and NULL-preservation contract needs an independent plan justification. |
| explain_analyze | Mixed lineage/JSON-schema additions and TopN/spill changes; whole transcript cannot be accepted as lineage-only. |
| explain_basic, explain_cardinality | TopN and JSON plan differences require individual plan justification. |
| join_explain_advanced | Twelve blocks include plan/JSON/lineage differences; blanket node-ID stripping would hide structural changes. |
| rowset_scan_pushdown | Access/late-materialization plan changes remain unproved. |
| select_topn_fallback_spill | Full sort no longer demonstrates the intended TopN fallback path. |
| vector_search, pgvector_topn_filter_flow | Provider/TopN replaced by scan/sort shapes; result equality does not establish search-path coverage. |
| optimizer_observability, search_optimization | TopN/ordering snapshots remain unproved. |
| statistics_query | RF artifact revision requires identity/consumer contract review, not unconditional hash erasure. |
| transaction_settings_savepoint | Full settings contract must explicitly register verifier/resource and read-only diagnostic entries. No added fields were hidden. |

The five-row `regress/cases/fulltext/fulltext_exec_mode_split.sql` is an existing
minimal production SQL reproducer for the unresolved TopK coverage issue. The
native TopN transformation and its matching path still exist; this record does
**not** claim that the cause is missing transformation, wrong cost or stopping
policy without a traced decision. Changing costs/budgets to recover the old
snapshot is outside this task. These unresolved plan decisions block C2.

## Integration checks

| Gate | Outcome |
| --- | --- |
| Workspace all-target locked check | PASS |
| Workspace tests | 6,846 passed, 0 failed, 85 ignored |
| Strict all-target Clippy, `-D warnings` | PASS |
| Benchmark + independent arithmetic/relation oracle tests | 192 passed; one existing pytest return-value warning |
| Regress harness tests | 101 passed, 1 skipped |
| Both 99-query result certification | OPEN: 95 exact + 1 bounded + 3 Uncovered per arm |
| Full SQL regress | OPEN: 166 pass / 18 fail; no blanket bless |
| New Q04/Q11/Q74 default-path performance baseline | NOT RUN: correctness gates remain open |

No performance improvement, sub-10ms result, parity, production readiness or
ProofComplete is claimed. Existing QualityPolicySatisfied and SearchIncomplete
remain distinct. The source, logs and artifact hashes are in the accompanying
v3 validation manifest; original mixed worktrees remain outside these commits.
