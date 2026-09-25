# Historical experiment index

Raw history was removed from the working tree, not from Git. All paths below
are relative to `benchmark/evidence/` at commit
`769ad610414a31094befc73d34e07d50e2e5ef14`. That snapshot contains the complete
6,112-file archive, including nested registrations, failed attempts and raw
results; an index row is not a new certification. Historical terminology and
thresholds apply only to their recorded revision.

Read a record without restoring its data:

```sh
git show 769ad6104:benchmark/evidence/<path>/README.md
```

Restore only a specifically needed experiment using `git archive 769ad6104
benchmark/evidence/<path>` into an explicitly owned directory. No history was
rewritten or garbage-collected by this cleanup. Removing current files does
not shrink reachable Git objects.

Routine runs now live in ignored `benchmark/runs/<run-id>/`; they are disposable
after review (normally within 14 days), not automatically purged. Commit only
a short consequential decision. Keep executable correctness fixtures under
`benchmark/corpora/` or tests. Formal evidence retention requires an explicit
owner/location, not this index alone.

Current design and unresolved reproduction instructions are in
[decisions](../../docs/optimizer/decisions.md) and
[open issues](../../docs/optimizer/open-issues.md). Q39 input SQL/spec moved to
[maintained contracts](../corpora/contracts/q39/README.md). TPC-H oracle
applicability is not closed by before/after result equality.

## Campaigns (historical excerpts, not revised verdicts)

Every row's source commit is `769ad6104`. README-indexed campaigns are listed
below; other standalone notes/raw siblings are retained in the same Git tree.

| Date | Original path / question | Historical outcome or scope excerpt |
| --- | --- | --- |
| 20260924 | `execution/20260924/aggregate-contracts-ab-v1` — Aggregate contracts: fresh control/probe comparison | Performance NotCertified; retain the original qualifications. |
| 20260924 | `execution/20260924/aggregate-contracts-v1` — Aggregate representation and ownership | Historical implementation/diagnostic record; no current performance certification. |
| 20260924 | `execution/20260924/q04-internals-v1` — Q04 scan/aggregate internals and placement validation | Performance NotCertified; retain the original qualifications. |
| 20260909 | `first-statement/q11/20260909` — Q11 first-statement evidence — 2026-09-09 | Historical implementation/diagnostic record; no current performance certification. |
| 20260910 | `first-statement/q11/20260910` — Q11 first-statement evidence — 2026-09-10 | Historical implementation/diagnostic record; no current performance certification. |
| 20260910 | `first-statement/q11/20260910/early-stop-v1` — Q11 early-stop handoff evidence | Historical implementation/diagnostic record; no current performance certification. |
| 20260911 | `first-statement/q11/20260911/incremental-pricing-v1` — Q11 candidate-combination incremental pricing | / 搜索档位 / Paro C1 / DuckDB C1 / C1 ratio（95% CI） / Paro warm / DuckDB warm / warm ratio（95% CI） / 实际停止 / 状态 / / --- / ---: / ---: / ---: / ---: / --- / / 20 ms / 174.932 / 109.619 ms / 1.588（1.557–1.616） / 110.157 / 105.749 ms /… |
| 20260911 | `first-statement/q11/20260911/region-coverage-v1` — Q11 per-region aggregate coverage and local quality follow-up | Historical implementation/diagnostic record; no current performance certification. |
| 20260911 | `first-statement/q11/20260911/same-memo-pready-v1` — Q11 same-Memo PReady → FrozenCandidate → execution | Historical implementation/diagnostic record; no current performance certification. |
| 20260911 | `first-statement/q11/20260911/strong-incumbent-isolation-v2` — Strong incumbent isolation v2 | Historical implementation/diagnostic record; no current performance certification. |
| 20260911 | `first-statement/q11/20260911/strong-incumbent-v1` — StrongIncumbentSeed v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260912 | `first-statement/q11/20260912/column-transfer-contract-v1` — Q11 selected-path column-transfer contract (2026-09-12) | Historical implementation/diagnostic record; no current performance certification. |
| 20260912 | `first-statement/q11/20260912/native-necessary-condition-v1` — Native necessary conditions: experiment registration | Implementation: c4c2eea9. Measured dirty source was based on 33ade4a9 with this patch plus the pre-existing mixed worktree. The six-file implementation commit intentionally excludes those pre-existing staged/unstaged dependencies; |
| 20260913 | `first-statement/q11/20260913/compile-work-closure-t0` — T-CWC / T0：干净基线构建阻塞 | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/e1-cold-t1-prod` — E1-COLD / T1-PROD — 2026-09-13 | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/e2-decouple` — E2-DECOUPLE — 2026-09-13 | **Pre-touch transfers the observed first-execution excess out of Q11.** In four adjacent fresh-process pairs, target latency falls33.29–49.28ms, with unchanged 1691 synthesis and nearly unchanged compiler time. Existing E1 scal… |
| 20260913 | `first-statement/q11/20260913/e3-admit` — E3-ADMIT — sequential materialization is not pure overhead | **The proposed direction gate fails.** With the existing codec gather primitive used for unmaterialized Sequential batches, Q11 C1 worsens193.674→202.664ms, warm91.221→112.150ms. Separate scalars show fill bytes161.930→108.724M… |
| 20260913 | `first-statement/q11/20260913/hygiene` — Hygiene: clean-source tests, no fixes or blessing | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/l1-ladder` — L1-LADDER: L0 stop, unequal physical work | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/local-domain-publication-v1` — Local necessary-domain publication: registered pilot | The candidate patch and its production regression test are preserved in candidate-not-admitted.patch. They were removed from production source after the default-path control exposed a severe regression. The release binary was |
| 20260913 | `first-statement/q11/20260913/local-domain-resume-v1` — Local-domain publication: bounded failed matching follow-up | Retained changes: |
| 20260913 | `first-statement/q11/20260913/m-power` — M-POWER: conditional sample size and formal T1 W gate | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/p1-frontier` — P1/P2 compiler cost investigation — no P3 admission | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/ub-prefix` — Authorized prefix preparation assay — withdrawn, U-BATCH still incomplete | Historical implementation/diagnostic record; no current performance certification. |
| 20260913 | `first-statement/q11/20260913/ubatch` — U-BATCH: contract blocker, not a measured performance negative | Historical implementation/diagnostic record; no current performance certification. |
| 20260914 | `first-statement/q11/20260914/memo-contract-v1` — Memo contract, fact lowering, and necessary-domain fixed point | Historical implementation/diagnostic record; no current performance certification. |
| 20260914 | `first-statement/q11/20260914/native-domain-journal-v1` — Native-domain rewrite journal pilot | / metric / Paro / DuckDB / / --- / ---: / ---: / / cold C1 median (ms) / 189.601 / 106.886 / |
| 20260914 | `first-statement/q11/20260914/native-shell-compaction-v1` — B3 native-shell compaction pilot | / metric / Paro / DuckDB / / --- / ---: / ---: / / cold C1 samples (ms) / 188.627, 194.718 / 112.594, 108.798 / |
| 20260914 | `first-statement/q11/20260914/node-publication-v1` — Node derivation and incremental publication | Retained code: **f47543cf**, **d463749b**, related tests; final verification source **79c00944**. The private settlement ColumnId/ScalarExprId/FactId catalog is NOT treated as the Memo catalog. Conversion still occurs through t… |
| 20260914 | `first-statement/q11/20260914/owned-facts-v1` — Owned bridge and fact reuse: T0–T2 | The late-payload/RowFetch hypothesis is rejected. All five proposed suspect commits independently produce c24392ee / 2053 / 1158, so drift predates them. The adjacent clean pair **f5adaed4 → f1ccd481** identifies an earlier actual |
| 20260914 | `first-statement/q11/20260914/predicate-transfer-bridge-negative-v1` — PredicateTransfer direct-only bridge: negative pilot | The PredicateTransfer direct-only optimization is rejected. It preserved the 90-row typed and ordered result, but removed a semantic peer/closure path that the Memo search still needs. On a clean worktree at 074174a2, the Q11 s… |
| 20260914 | `first-statement/q11/20260914/predicate-transfer-owned-bridge-v1` — PredicateTransfer owned-IR bridge reduction | Historical implementation/diagnostic record; no current performance certification. |
| 20260914 | `first-statement/q11/20260914/preflight-layout-v1` — Q11 B3 preflight and native-layout reuse v1 | The B3 hypothesis is implemented and passes targeted semantic checks. The pilot is consistent with a small C1/compiler reduction, but it is not a causal or formal performance result. The remaining normal gap is about 94.7ms |
| 20260914 | `first-statement/q11/20260914/structural-preflight-v1` — Q11 B3 structural preflight v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260914 | `first-statement/q11/20260914/work-partition` — Optimizer exhaustive partition and early quality-request rejection | The requested partition gate passes. The B5+B10=25–35ms hypothesis is rejected: these scopes total3.889ms, not the main missing cost. Quality work totals18.276ms, including8.243ms producing requests that can subsequently be rej… |
| 20260915 | `first-statement/q11/20260915/execution-layers-v1` — Q11 execution-layer architecture pilot v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260915 | `first-statement/q11/20260915/low-allocation-cost-v1` — Q11 low-allocation physical cost synthesis v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260915 | `first-statement/q11/20260915/native-factory-v1` — Native Memo factory / resident publication v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260915 | `first-statement/q11/20260915/numeric-decode-v1` — Q11 numeric-page decode attribution v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260915 | `first-statement/q11/20260915/page-read-ownership-v1` — Page-read ownership and decode profile (2026-09-15) | This round targets the execution-side path only: |
| 20260915 | `first-statement/q11/20260915/quality-evidence-v1` — Q11 quality-evidence incremental-composition v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260915 | `first-statement/q11/20260915/resident-identity-v1` — Q11 planning-resident identity v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260916 | `first-statement/q11/20260916/goal-scoped-physical-deps-v1` — Goal-scoped physical dependencies v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260916 | `first-statement/q11/20260916/native-compute-physical-state-v1` — Q11 native property construction and resident physical state v1 | The two requested production contracts are implemented and separately committed, but the compiler target is not met. A removes a real native owned-IR property-construction round trip; B preserves exact physical progress and |
| 20260916 | `first-statement/q11/20260916/native-relation-facts-v1` — Native relation-facts reuse v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260916 | `first-statement/q11/20260916/quality-preflight-v1` — Q11 on-demand quality preflight v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260916 | `first-statement/q11/20260916/response-delta-v1` — Q11 response-delta physical progression v1 | Historical implementation/diagnostic record; no current performance certification. |
| 20260917 | `first-statement/q11/20260917/cross-group-domain-continuation-v1` — Q11 cross-group domain continuation v1 | The Q11 diagnostic lane used four selected PredicateTransfer bindings. An exploratory shape probe (not used as performance evidence) showed that their paths already expand all transparent intermediate operators and terminate at |
| 20260917 | `first-statement/q11/20260917/cross-group-domain-continuation-v2` — Cross-group domain continuation: root-contract diagnosis | Historical implementation/diagnostic record; no current performance certification. |
| 20260917 | `first-statement/q11/20260917/n-chain-normalize-v1` — Q11 N-CHAIN: pre-Memo CTE domain normalization | This experiment does not justify enabling the chain in production. |
| 20260917 | `first-statement/q11/20260917/native-relation-edge-sharing-v1` — Native relation completion evidence: edge sharing pilot | / measure / Paro / DuckDB / note / / --- / ---: / ---: / / cold C1 median (ms) / 159.989 / 114.332 / |
| 20260917 | `first-statement/q11/20260917/obligation-lane-v1` — E-LANE: obligation-only quality-lane experiment | Historical implementation/diagnostic record; no current performance certification. |
| 20260924 | `optimizer/20260924/direct-pipeline-v1` — Direct pipeline vertical slice | The three implementation steps are present: preserve Q74's valuable decisions, plan a committed relation tree outside Memo, and test semantic/resource boundaries. The six-cell replacement matrix passed complete result, type, |
| 20260924 | `optimizer/20260924/regional-cost-v1` — Shared regional work and predicate activation | Historical implementation/diagnostic record; no current performance certification. |
| 20260924 | `optimizer/20260924/regional-pipeline-v1` — Finite regional pipeline pilot — not promoted | Keep the slice explicitly selectable for continued migration, not the default. Next work must introduce maximal-region ownership and a closed, contextual physical interface, and determine the Q74 candidate-quality loss. Adding … |
| 20260925 | `optimizer/20260925/join-predicate-contract-v1` — Join contracts and corpus-driven window execution | The corpus-priority recommendation was useful. Treating every Filter above a join as an error was not: outer/reduction boundaries, multi-input expressions, OR, projection namespaces and evaluation fences must remain explicit. |
| 20260925 | `optimizer/20260925/joint-grain-v1` — Joint aggregate grain / join-subset planning | Historical implementation/diagnostic record; no current performance certification. |
| 20260925 | `optimizer/20260925/pipeline-default-v1` — Pipeline default and window workspace | Historical implementation/diagnostic record; no current performance certification. |
| 20260925 | `optimizer/20260925/planner-cleanup-v1` — Planner cleanup validation | Historical implementation/diagnostic record; no current performance certification. |
| 20260925 | `optimizer/20260925/region-kernel-v1` — Regional response kernel: evidence and limits | Historical implementation/diagnostic record; no current performance certification. |
| 20260925 | `optimizer/20260925/relation-convergence-v1` — Relation convergence: implementation and negative-result ledger | Historical implementation/diagnostic record; no current performance certification. |
| 20260925 | `optimizer/20260925/single-planner-v1` — Single planner ownership validation | - Workspace: 6,368 passed, 85 ignored; strict Clippy and workspace check passed. - Real Q04 and SELECT 1 PgWire Detail captures validate as v4 with four ordered stage completions; their durations are diagnostic, not latency evi… |
| 20260920 | `optimizer-convergence/20260920` — Optimizer convergence execution record | Historical implementation/diagnostic record; no current performance certification. |
| 20260923 | `optimizer-migration/20260923/cardinality-ownership-v1` — Cardinality ownership recovery | Historical implementation/diagnostic record; no current performance certification. |
| 20260923 | `optimizer-migration/20260923/construction-bounds-v1` — Change-driven construction, compact quality evidence and demand-driven bounds | The changes preserve the admitted artifact, physical selection, grant and cost synthesis counts in all three queries. Q04 has a directional compiler reduction; Q11 is flat and Q74 is slightly slower. This does **not** establish… |
| 20260923 | `optimizer-migration/20260923/construction-dag-v1` — Canonical construction, selected DAG and physical observations | All three ownership/construction changes are implemented, without changing search budgets, stop policy, costing, execution or verification settings. This pilot does **not** show a broad compiler speedup. Q11 improved modestly; |
| 20260923 | `optimizer-migration/20260923/physical-core-v1` — Physical search core: implementation and evidence | Historical implementation/diagnostic record; no current performance certification. |
| 20260923 | `optimizer-migration/20260923/preparation-v1` — Demand-driven compile preparation: Q04/Q11/Q74 | All three architectural changes are implemented. This campaign **does not demonstrate a Q11 compiler improvement or a 50% reduction**. Q74's aggregate compiler median is lower; Q04 and Q11 are slightly higher. Retain the negative |
| 20260923 | `optimizer-migration/20260923/property-ownership-v1` — Selected properties and physical-subproblem ownership | All three ordered interventions are implemented. The substantial measured benefit is Q74: normal compiler median **72.55 -> 40.22 ms (-44.6%)** with the same selected/admitted identities, resource contracts, logical/physical co… |
| 20260923 | `optimizer-migration/20260923/remaining-contracts-v1` — Remaining historical contracts: matrix result | Historical implementation/diagnostic record; no current performance certification. |
| 20260924 | `optimizer-migration/20260924/q04-c1-round2` — Q04 DECIMAL fusion: interrupted fresh comparison | Performance NotCertified; retain the original qualifications. |
| 20260924 | `optimizer-migration/20260924/q04-linear-decimal-v1` — Q04: certified linear DECIMAL execution | Historical implementation/diagnostic record; no current performance certification. |
| 20260924 | `optimizer-migration/20260924/selected-local-properties-v1` — Demand-driven selected properties | Q74's repeated quality work is reduced; normal compiler median falls 8.5%. Q04 is essentially flat and Q11 does not improve. C1 does not improve consistently. This is not Q11 <10ms, overall compiler convergence, warm |
