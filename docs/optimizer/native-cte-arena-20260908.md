# Native CTE requirements, settlement and logical arenas

This delivery replaces the remaining search-time CTE subtree adapters and
connects node-local settlement to the arena representation. It is a structural
delivery, **not a completed cold-planning or Q11 performance optimization**.
The final measured Q11 plan is slower than the previous checkpoint and DuckDB;
the evidence and remaining boundaries are recorded below.

## Ownership and facts

`LogicalPlan` now owns an immutable `LogicalPlanArena` and a root `PlanIndex`.
An edge contains an index, not an owned descendant. Indices include arena and
non-reusable generation identities; rollback cannot revive an unpublished
handle. Iterative import/export admits or cancels work before expanding each
evaluation occurrence. A shared 33-node diamond DAG is therefore not silently
expanded into billions of owned nodes under an eight-unit export allowance.

The old type is explicitly named `OwnedLogicalPlan`, without a compatibility
alias. It remains the mutable binder/baseline IR, the bounded local semantic
rule window, and the final execution-occurrence boundary. This is **not** a
claim that every binder and execution-facing API is arena-native. Memo payloads
contain only `LogicalPlanNode<()>` shells. Settlement, root-layout freezing,
verification and transformed-expression staging carry the arena directly;
staging builds each node from immediate child facts rather than rebuilding a
representative descendant tree. Physical search providers export only their
admitted Filter/TopN scan windows.

Boundary facts carry separate expected NDV, hard distinct bounds, immutable
typed value domains, NULL-safe grouping keys, source coverage and replay
evidence. Column statistics are positional and occurrence-owned. A derived
column estimate no longer allocates an empty HLL that silently overrides the
explicit estimate. Local boundaries cap expected NDV by their expected row
domain without inventing a guaranteed upper bound. Finite equality domains
publish both their expected cardinality and their independent semantic bound.

## CTE requirements

A native requirement identifies the owner, exact producer group, definition,
materialization policy and sharing region. Each consumer has its own edge
path, output rebinding, predicate/key demand and null-extension ownership.
Consumer paths, not reused table aliases, identify partition assignments.

Inline, shared-domain and partitioned alternatives preserve producer GroupRefs
and compete in the owner's Memo group. Their implementation cannot consume a
representative producer and freeze out later producer alternatives. An
unrestricted occurrence prevents producer-domain restriction. Splitting shared
evaluation needs replay evidence; replaying the same input does not grant
permission to commute a potentially failing expression across row removal.
Aggregated SUM predicates stay above aggregation unless an ordinary local
rule separately proves a transfer.

Domain symbols isolate the statistics environments of different restrictions.
The identity of successive restrictions is commutative and idempotent:
`A; B`, `B; A` and `A; B; A` share one domain symbol. Separate key-UNION proofs
remain separate conjuncts when composed. SQL materialization policy is never
used as a hidden "already optimized" bit. Old producer-tree demand and
partitioning implementations have been removed; predicate algebra is retained
as a pure helper.

## Incremental settlement

The cache key is an immutable local operator/scalar recipe, ordered input fact
identities, output identity and the lexical CTE domain actually consumed.
Identical fact values reuse a local recipe across new evaluation occurrences.
Statistics, output demand and execution demand are reduced at node boundaries;
UNION inputs retain their positional contracts even when aliases coincide.

Boundary revalidation caches persistent DAGs of actual fact reads. It validates
both positive and negative evidence, including root facts, and admits work
before extending the read set. An unchanged derivation does not recollect a
whole representative tree. Scope materialization likewise visits distinct
Memo groups and checks its complexity ceiling rather than unfolding shared
scope ownership recursively.

## Search closure contract

Physical contexts declare the source work an ancestor may filter. Dominance
preserves that parent-visible response and output task supply. The arbitrary
eight-winner truncation is gone: admitted non-dominated candidates survive,
and parents retain exact immutable child candidate identities independently
of later frontier pruning.

Child products use a lazy mixed-radix iterator. Construction is linear in the
input frontiers, not their Cartesian product. An omitted combination publishes
a `SearchObligation`; advisory rule failures also prevent a false Complete
status. Required baseline planning remains available under optional exhaustion.

An independent finite oracle checks all nine combinations in a two-input
rewrite domain. Twenty-four rule-ID, group-order, seed-insertion and fingerprint
permutations reach the same closure and minimum cost with sufficient budget.
The zero-budget counterpart retains its baseline and reports the omitted rule
class. Further tests cover more than eight non-dominated winners, exact child
references after pruning, huge child products, CTE partition discriminators,
reused aliases, replay guards, domain composition and rollback.

This proves the tested search/combination contracts, not a theorem that every
production query has reached an unbounded global optimum. Default-budget Q11
still reports `search_complete = 0`; it must not be presented as a closed
optimum.

## Validation

Validated source: `42ccdbef1b84bd7d020229562813fa1cd6177cbf`, clean worktree.

- `make static`: passed, including workspace/all-targets Clippy.
- Workspace tests: 6,368 passed, 85 ignored, no failures on the final full run.
  The first full run had one graph-refresh admission failure; the same test
  binary passed three isolated reruns, the targeted Cargo rerun passed, and
  the subsequent full workspace run passed. No graph implementation or test
  expectation was changed to suppress it.
- Optimizer/planner library tests: 936 and 265 passed respectively.
- SQL regress: 183/183 passed after reviewing all seven complete transcript
  differences and updating only their EXPLAIN expectations. Query results are
  unchanged. The singleton-group case conservatively retains a partial-merge
  aggregate: its old tree-shaped witness is not manufactured from a group hole.
- Both local server builds were refreshed. Regression writes used a separate
  temporary data directory; comparator timing ran without concurrent builds,
  tests or regression runners.

Local logs are `/tmp/paro-native-contracts-workspace-tests-rerun-20260908.log`
and `/tmp/paro-native-contracts-regress-verified-20260908.log`.

Three additional fresh server processes produced byte-identical Q11 EXPLAINs:
SHA-256 `8c79411091b299f700d1f49b1643b24746cdaec0955fe2f0c4f9e4c3b843b3bc`.
All three reported 1,016 groups, 1,614 logical expressions, zero rule failures,
12,640 settlement hits and 2,408 misses. Plans and counters are retained in
`/tmp/paro-native-arena-explain.STPJwE/`.

## Q11 evidence and remaining performance work

TPC-DS SF1, four threads, 2 GB, `optimizer_verify=true`, symmetric `none`
metadata. Five fresh-process blocks, two warmups and three ABBA rounds per
block produced 30 measured samples per engine. All samples matched the complete
90-row result, schema and ordering contract.

| Measurement | Paro | DuckDB |
|---|---:|---:|
| Steady execute/fetch median | 121.731 ms | 105.002 ms |
| First complete statement median | 1,996.586 ms | 108.584 ms |

Paired geometric ratio: **1.158340**, hierarchical 95% CI
**[1.150173, 1.166666]**. This is a measured regression, not a performance win.
The first-statement number includes compile, execute, fetch and result metadata;
it is not a pure planner microbenchmark.

Binary SHA-256:
`343f83265f47d4a4d797ee74ab97da963c1db14ac6b944052872dfab1c56f029`.
The complete attested report is
`benchmark/report/native-cte-arena-final-20260908.json` (intentionally ignored
by Git). The previous diagnostic and failed experiments are retained separately.

The chosen plan retains year-domain pushdown and narrow fact aggregation, but
uses separate customer joins rather than the shared dimension candidate.
Wider native combination exposes more search work: the diagnostic records
about 6.06 GB of allocation traffic, 12,640 settlement cache hits and 2,408
misses. Allocation traffic is not peak live memory. There are no advisory rule
failures, but local/composition work, optional groups and child combinations
still exhaust their budgets.

The observed incomplete dimensions are explicitly:

| Budget dimension | Exhaustion events in each fresh EXPLAIN |
|---|---:|
| Optional group | 173 |
| Composition group | 19 |
| Local rule work per group | 2,815 |
| Composition rule work per group | 28 |
| Composition rule fires per group | 10 |
| Child frontier combination | 136 |

These are known incomplete classes, not missing-work counts or a waiver that
would classify this query as Complete. New failure classes must be investigated.

The next performance work is evidence-sensitive cardinality composition and
avoiding redundant requirement/pattern work, not smaller budgets or Q11-shaped
cost constants. The missing singleton scalar lowering also needs to consume a
replayable native NULL-safe uniqueness witness. Restoring a tree materializer
or assuming a selected producer would defeat the ownership contract delivered
here. Neither the broader cold-latency target nor faster-than-DuckDB Q11 is
claimed complete by these commits.
