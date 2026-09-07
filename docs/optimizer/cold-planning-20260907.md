# Cold planning and Q11: 2026-09-07 checkpoint

This checkpoint closes the deterministic-search and Memo fact-replay defects
found in the `adb0440e` review. It is still **not** completion of the
cold-planning performance target: fresh-process Q11 statement latency remains
materially slower than DuckDB, and the current steady-state confidence interval
does not prove that Paro is faster.

## Contracts implemented after `adb0440e`

- Logical filter generation and conjunct selectivity are order invariant.
  Equivalent predicate sets therefore produce the same estimates and plan
  search, independent of randomized hash-map iteration.
- Rule replay first revalidates the immutable fact value consumed by the prior
  application. A changed derivation cursor with an equal value refreshes the
  subscription without reconstructing and reapplying the binding.
- Stable fingerprints batch scalar writes into one BLAKE3 transcript. BLAKE3 is
  retained deliberately: several fingerprints are proof and scheduler
  identities, so replacing them with a fast bucket hash before separating
  bucket identity from proof identity would weaken a correctness contract.
- Root dispatch rejects aggregate rules whose immutable operator payload cannot
  match. Associative join bindings are deduplicated by their normalized graph
  and resolved fact values before rule admission.
- Winner verification traverses the retained candidate frontier rather than
  every discarded physical expression. Search work, binding construction, and
  fact-value cache hits/misses are published as stable optimizer counters.
- Scalar-aggregate, aggregate post-reduction, join-elimination, and CTE filter
  demand rules bind only their consumed operator paths. Opaque descendants stay
  as exact Memo group references with transported cardinality, uniqueness,
  lineage, and control-region facts.
- Memo groups cache immutable logical-fact and statistics identities. Mutation
  invalidates the cache at the group boundary; repeated reads no longer rebuild
  identical transcripts.
- Relational rewrites may discard an opaque input (for example, eliminating the
  unique side of an outer join). Every surviving group reference must remain
  registered exactly once and is atomically substituted during staging;
  introduced or duplicated references are rejected.

No Q11 fingerprint, query-specific cost, reduced search budget, compatibility
path, or cached-statement timing is used in this delivery.

## Validation

- `make static`: passed, including workspace/all-targets Clippy.
- `cargo test --workspace --no-fail-fast -j4`: passed after the final
  join-elimination boundary fix, including 914 optimizer tests and all
  documentation tests.
- SQL regress against a fresh data directory: **183/183 passed**.
- Three EXPLAIN artifacts changed only because the stable fingerprint transcript
  received a new domain; their plan structure and results are unchanged.
- The outer-join elimination regression exposed by path binding is fixed using
  transported Memo uniqueness, without reconstructing a representative child
  tree.

## Current Q11 evidence

Measured clean source: `4be4fe56446258c309dc93794fa02c7feab4a57d`.

Binary SHA-256:
`a741ba5ad32e3cdadbcc687d877a2d9b267710bf8fcd3d7ff30201fadeb5c1a9`.

TPC-DS SF1, four threads, 2 GiB, symmetric `none` metadata and
`optimizer_verify=true`. The comparator used five fresh-process blocks, two
warmups per process and three ABBA rounds per block. All 30 measured samples per
engine matched the complete 90-row multiset, schema and ordering contract.

| Measurement | Paro | DuckDB |
|---|---:|---:|
| Steady execute/fetch median | 106.645 ms | 106.910 ms |
| First complete statement median | 984.773 ms | 110.929 ms |

The steady geometric paired ratio is **0.980299**, with hierarchical 95% CI
**[0.895510, 1.059492]**. The point estimate favors Paro, but the interval
crosses one, so this run does not qualify as faster. The fresh-process result is
about 8.9 times DuckDB and includes compile, execute, fetch, and result metadata;
it must not be described as pure planning latency.

For comparison, the reviewed `adb0440e` source produced 4.7--9.7 second cold
EXPLAIN observations and multiple plan shapes. The current source produces a
stable plan and roughly one-second first statements. A debug diagnostic reports
779 groups and these principal rule attempts: aggregate dimension deferral 531,
aggregate join subsumption 468, and join-region enumeration 534. Search remains
budget-limited.

Local evidence (benchmark reports are intentionally git-ignored):

- `benchmark/report/tpcds-q11-cold-final-20260907.json`
- `benchmark/report/tpcds-q11-cold-final-20260907.q11.*.parod.log`

## Remaining structural work

Three transformations still use the complete-subtree adapter: CTE inlining,
CTE demand pushdown, and partitioned CTE materialization. A mechanical group-hole
migration is unsound: a CTE consumer occurrence has rebinding, predicate-demand,
null-extension, and sharing-owner semantics that a relational GroupRef does not
encode. Experiments with that migration either lost Q11's shared producer plan
or expanded the search frontier, so they were not retained.

The next long-term boundary is a native CTE requirement value. It must carry a
producer identity plus the complete set of consumer occurrence demands and
scope/null-extension proofs. Inline, shared, and partitioned implementations can
then compete under one requirement while preserving exact producer GroupRefs.
Only after those three rules migrate can the generic `PatternScope::Subtree`
adapter be deleted.

The remaining cold gap also requires incremental shell settlement: successful
local rewrites currently repeat filter normalization, statistics gathering,
column lifetime analysis, and verification over every retained shell path.
Those passes need fact-keyed node results and invalidation, not a lower global
budget. Acceptance remains: identical closure under rule/expression order
permutations when budgets suffice, explicit incomplete status when they do not,
byte-identical repeated EXPLAIN, fresh-process comparison, and a newly qualified
Q11 execution result.
