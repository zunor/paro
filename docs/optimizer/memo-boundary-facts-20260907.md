# Memo boundary facts: 2026-09-07 checkpoint

This is a partial delivery of the native-facts / no-whole-tree-reconstruction
task. The boundary-facts implementation and several pattern migrations are
complete; **the overall removal and cold-planning targets are not complete**.
Q11 result correctness passed, but the current benchmark does not establish an
execution advantage over DuckDB and cold statement latency regressed.

## Implemented contracts

- `444aba27`: decline dimension deferral when its proposed partial grouping
  already covers an input key. This prevents unbounded partial/merge layering
  after settlement exposes the first partial aggregate's intrinsic key.
- `6b2c964c`: derive cardinality, unique keys, source-column coverage and
  control-region evidence directly over Memo groups and operator shells.
  No representative plan is reconstructed to obtain these boundary facts.
- `BoundRelationFacts` is immutable transport evidence. Output schemas are
  checked at the boundary; unique keys are derived by the same one-operator
  algebra used by ordinary statistics propagation. Transported keys retain
  structural provenance, not a fabricated catalog-enforcement guarantee.
- Key proofs may be established by an equivalent expression. Source coverage
  instead requires agreement across **every** alternative. Partial/unknown
  paths and repeated UNION ALL source identities decline coverage; they do
  not manufacture a runtime-filter proof. Physical publication / bypass
  coverage verification remains mandatory and unchanged.
- Fact traversal is iterative, DAG-memoized and cancellation-aware. Evidence
  work is admitted before constructing its operands/results. Exhaustion
  survives optional-rewrite rollback and leaves the baseline available.
- Local derivations are cached by their own read cursor and input fact
  objects. Re-derivation that produces the same value preserves the immutable
  object, allowing parent derivations to reuse unchanged evidence.
- A shallow `PatternRead` no longer hides a recursive cardinality traversal.
  The fact reader records the actual inherited/producer dependencies instead.
  Application-only reads remain subscribed across negative matches, failed
  optional rewrites and later bindings with disjoint read sets.
- CTE registry changes are evidence changes, including the first producer
  registration after an unresolved consumer. The registry and wake-up set
  participate in rollback.
- Join-graph reuse compares resolved fact values. A statistics recipe rename
  or additional derivation with unchanged values is not a new graph problem.

## Pattern migration completed in this checkpoint

Join-region enumeration now keeps non-reorderable relation inputs as group
references. Dimension deferral / aggregate-input materialization bind their
projection and inner-join region, preserving the remaining input groups.
Late payload fetch binds only its proof-supported row-id path and is invoked
at the matched root. Detail-aggregate subsumption binds the join/exposure/
partial-aggregate skeleton, not arbitrary subtrees attached to it.

The shell adapter assembles explicit operands iteratively. Settlement still
operates on those bound shells. The strict group-hole guard remains in place:
unregistered, discarded, duplicated and ill-typed occurrences are not silently
accepted. The baseline detail-subsumption walker also uses the canonical
post-order traversal instead of recursively moving a wide LogicalPlan enum.

## Validation

- `make static`: passed, including workspace/all-targets clippy.
- `cargo test --workspace --no-fail-fast -j4`: **6,335 passed, 85 ignored,
  zero failed**, including documentation tests.
- Optimizer unit tests: **910 passed**.
- SQL regress, fresh data directory and default memory configuration:
  **183/183 passed**, no baseline changes.
- The first regression invocation used an explicit 2GB server setting and
  produced two setting-output mismatches against the default 1GB baseline.
  Correcting the invocation, not the baselines, resolved them.

New tests cover a nine-group shared DAG instead of its 511-node tree
expansion; intrinsic aggregate keys and aliases; conservative alternative
coverage; repeated source identities; budget exhaustion; inherited evidence
invalidation; cache value reuse; CTE registration / rollback; actual engine
negative-match wake-up; preservation of earlier binding subscriptions; graph
recipe/value distinction; bounded subsumption patterns; and deferral
idempotency. Existing n-ary dimension-sharing budget-stability assertions
remain intact.

## Q11 measurement and limitations

Measured clean source: `6b2c964c83617183e4256fa080731322b788ee61`.
Binary SHA-256:
`3c77bd1c535d06f403cb1dc043ecc5125e3dadc08113e01c67806af3ea983c3e`.

TPC-DS SF1, DOP 4, 2GB, symmetric `none` uniqueness metadata,
`optimizer_verify=true`. Five fresh-process blocks, two warmups per process,
three ABBA rounds per block, 30 measured samples per engine and 10,000
hierarchical bootstrap resamples. All measured samples matched the complete
90-row result, schema and ordering contract.

| Measurement | Paro | DuckDB |
|---|---:|---:|
| Steady execute/fetch median | 121.044 ms | 112.118 ms |
| First complete statement median | 10,121.614 ms | 112.274 ms |

The geometric paired ratio is **1.009248**, hierarchical 95% CI
**[0.850557, 1.151556]**. This is not the ratio of the two aggregate medians.
Block ratios were 0.961304, 0.972985, 1.207715, 0.867943 and 1.067987.
Neither an advantage nor equivalence is established by this interval.

The earlier within-turn diagnostic also failed the performance criterion:
ratio 1.046529, CI [1.000183, 1.105221], first-statement median 10,145.870 ms.
It is retained, not replaced by a claim that the final sample proves a win.
The previous checkpoint's 3,362.762 ms first-statement median is substantially
lower than this checkpoint. First-statement timing includes execution and
must not be labelled pure planning latency.

A separate single-run profile of the clean source reported 6,681,853 us in
Memo exploration, 7,674 join-region attempts, 870 groups and 1,329 logical
expressions. Optional search was incomplete under its unchanged budget.
This profile identifies remaining work; it is not an independent benchmark.

Local evidence (generated reports are intentionally git-ignored):

- `benchmark/report/tpcds-q11-memo-facts-6b2c964c-20260907.json`
- `benchmark/report/tpcds-q11-memo-facts-final-20260907.json` (earlier diagnostic)
- `/tmp/paro-memo-facts-workspace-final-tests-20260907.log`
- `/tmp/paro-memo-facts-static-final-20260907.log`
- `/tmp/paro-memo-facts-final-regress-results-20260907.log`
- `/tmp/paro-memo-facts-clean-query-20260907.log`

## Work still required before removing all reconstruction

Seven production transformations still use `PatternScope::Subtree`:
CTE partitioned materialization, CTE inlining, CTE demand pushdown, CTE filter
pushdown, aggregate post-reduction, join elimination and scalar-aggregate
window rewriting. The full-subtree fallback has **not** been deleted.
Typed shell instantiation is therefore not yet equivalent to a fully native
Memo transformation interface.

The next structural contracts are:

1. A CTE-use summary with complete positive and negative reads, predicate
   demand and scope/null-extension boundaries. Producer restriction must
   stage a filter and preserve its producer GroupRef, with local predicate
   propagation instead of whole-producer settlement.
2. An explicit relation-instance / rebinding contract for inline and demand
   copies. A GroupRef denotes equivalent relational semantics; it does not
   by itself authorize reusing one occurrence's bindings, execution region
   or source-work identity in another occurrence. Do not weaken the current
   duplication guard to force these rewrites through.
3. Memo-native source equivalence with an output-column bijection and exact
   bound-kernel/evaluation obligations. Alpha-source rules cannot replace
   their current proof with matching table names, fingerprints or NDV.
4. Rule-local mutation and incremental settlement for the remaining wrappers;
   remove the generic full-subtree fallback after migrating every caller.
   Fact-value invalidation should drive this work instead of repeatedly
   revisiting complete evidence/pattern DAGs for unchanged values.

Acceptance still requires independent closure/oracle tests across insertion
and rule order, explicit limited-search diagnostics, fresh-process EXPLAIN
comparison, and a newly qualified Q11 execution result. No query-specific
predicate, cost constant, budget reduction, compatibility switch or cached
statement timing substitutes for these obligations.
