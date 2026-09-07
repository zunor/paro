# Cold planning and Q11: 2026-09-07 checkpoint

The subsequent structural migration and its separate performance evidence are
recorded in [Native CTE requirements and logical arenas](native-cte-arena-20260908.md).
The measurements below describe this historical checkpoint.

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

## Review follow-up: measured ownership, not inferred ownership

The follow-up implements the observability prerequisite for further structural
work instead of changing a search constant:

- The server allocator now supports scoped, thread-local allocation-volume
  measurement. Accounting is enabled only while the synchronous optimizer is
  running; ordinary execution does not update the byte counter. Each compiler
  component and each transformation rule publishes allocated bytes.
- Binding construction and rule application publish elapsed time per rule.
  Budget-limited pattern enumeration, work admission, fire admission and output
  reservation publish the responsible rule, rather than only a global counter.
- `DistinctStatistics` caches the HLL estimate at mutation/deserialization
  boundaries. Memo readers no longer rescan the sketch for an immutable fact.
- Memo boundary derivation carries exact finite grouping domains and a separate
  SQL-grouping-safe uniqueness proof. The separation is required because a
  nullable catalog UNIQUE key is not necessarily unique under `GROUP BY`, where
  NULL values compare equal. Disjoint `UNION ALL` branch domains can now prove a
  structural key without a Q11-specific rule observation. Collated strings,
  floating point, NULL and nested values are deliberately outside this proof
  domain.
- These new proofs do not participate in an unrelated runtime-artifact identity.
  Proof identity and physical artifact identity remain separate domains; an
  early implementation that mixed them changed runtime-filter identifiers and
  was rejected.

On Q11 the final diagnostic attributes about 3.20 GiB of allocation traffic to
Memo exploration. The largest rule owners are join-region enumeration
(~661 MiB), aggregate dimension sharing (~302 MiB), aggregate dimension
deferral (~253 MiB), aggregate input materialization (~100 MiB), and aggregate
join subsumption (~72 MiB). This is allocation *volume*, not live memory or a
resource grant. It establishes concrete ownership for arena/incremental
settlement work and avoids treating malloc samples as a design proof.

The same run records rule-local budget exhaustion: CTE demand pushdown 116,
CTE filter pushdown 96, aggregate join subsumption 84, aggregate dimension
deferral 60, aggregate post reduction 6, dimension sharing 3, and one each for
CTE inline/partitioning. `search_complete` remains zero. The optimizer therefore
returns an explicit deterministic anytime result; this checkpoint does not
mislabel it as a closed optimum.

Two tempting migrations were tested and rejected:

- Mapping all CTE transformations onto the existing CTE reference path reduced
  Q11 matching work, but changed a regression EXPLAIN cardinality from one to
  zero. A relational group hole cannot encode producer identity, occurrence
  demand, null-extension proof and sharing ownership, so the native CTE
  requirement described above remains necessary.
- A generic singleton-group aggregate elimination rule did not see the Q11
  aggregate in the required transformation frontier and merely added attempts.
  The grouping proof is retained as the correct fact foundation; elimination
  must consume that fact through incremental settlement rather than adding a
  second tree rewrite.

Final fresh-process comparator evidence is stored in
`benchmark/report/tpcds-q11-remaining-gaps-scoped-metrics-20260907.json`. Thirty
measured samples per engine produced identical result hashes. Paro's steady
median was 104.694 ms and DuckDB's 105.522 ms; the paired ratio was 0.994236
with hierarchical 95% CI [0.985280, 1.002613]. The point estimate favors Paro,
but the interval crosses one, so this run does not qualify as faster. First
complete statement medians were 939.362 ms and 107.739 ms respectively. The
remaining target is therefore still open; neither allocation instrumentation
nor a favorable historical sample is used to claim completion.
