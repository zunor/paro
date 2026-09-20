# Compile observation baseline (T0)

Scope: T0/T1 only. T2 admission/ANALYZE, T3 Detail, T4 retirement and T5
campaign integration are not claimed. No behavior experiment is retired here.

## Source and recoverability

Integration checkpoint `55f00d33` merges committed re-op `955c7d81` into the
existing convergence branch (`335c5c8f`). The five re-op-only commits change
guidance/workflows, not Rust production sources. Main-tree mixed hunks were not
integrated. This is a review checkpoint, not C2/F2 product admission.

Private recovery set:
`/Users/linjunhong/paro-convergence-archive/20260920/trace-t0-A1tvuO/`.
`main/` and `docs/` separately retain HEAD, original index, staged binary patch,
unstaged binary patch and only untracked files. `main-audit.json` and
`docs-audit.json` verify reconstructed content with a separate Git index:
133 and 17 files respectively. Unresolved ownership remains with the user;
none of these private bytes is added to Git. No new worktree/data copy was made.

The unchanged production crate tree is certified by the prior clean final
manifest `benchmark/evidence/optimizer-convergence/20260920/search-validation-manifest.json`:
6854 tests pass, strict Clippy/check pass; regress has 8 unresolved contracts.
This is explicitly reused evidence, not a new run. An attempted T0 optimizer
rerun was interrupted when T1 source editing began; its log is NOT baseline
evidence. T1 requires its own fresh clean validation.

## Emitter and boundary mapping

| Owner/source | Existing boundary | T1 mapping / limits |
| --- | --- | --- |
| session execute | parse, cache, statement scope | request COMPILE bypasses target lookup/publish; ForcedCompile, not CacheMiss; parse Uncovered until a real shared span is available |
| compiler compile | parsed AST entry, bind, optimize, verify, deferred program | one target call; observed intervals and ready portfolio, never execution-image-ready |
| optimizer OptimizationOutput | rule attempts/insertions/time, search stop, obligations | copy bounded scalar summaries at existing extraction return, no re-evaluation |
| work_partition / b3 | exclusive nested activity scopes | retained legacy source; fine-grained migration not yet complete, no invented zero fields |
| TaskRegistry / Memo | task/fact/frontier transitions | T3, not copied wholesale for Summary |
| execution explain | diagnostic rendering | same immutable record to bounded TEXT/JSON; no recursive compile |
| session sink / vector owner | output/backpressure | retain capture reservation through referenced result storage |
| portfolio/admission | expected vs actual class, lazy lowering | only expected summary in T1; actual admission/execution NotExecuted; F2 unadmitted |
| benchmark corpora readers | legacy trace and normal scalar receipts | T4; no reader or legacy sink deleted by T1 |

## Control classification and retirement ownership

The source registry is `context/diagnostic_environment.rs`; actual read sites
must be distinguished from its observation-only name list. Unknown or absent
controls are not permission to delete downstream readers.

| Names | Class / effective behavior / owner | Replacement and retirement gate |
| --- | --- | --- |
| PARO_STATEMENT_TRACE, PARO_STATEMENT_TRACE_SAMPLE | DiagnosticOutput; enabled flag and sample identity; context/session | T4 after invocation/lifecycle and bounded consumer parity |
| PARO_DIAGNOSTIC_WORK_PARTITION | DiagnosticOutput; presence/path enables exclusive profiling; optimizer work_partition | T4 after all activity boundaries and harness consumers migrate |
| PARO_DIAGNOSTIC_COST_PHASE_TIMES | DiagnosticOutput; optional phase timing; engine | T4 timing coverage equivalence |
| PARO_DIAGNOSTIC_FRONTIER_SNAPSHOT | DiagnosticOutput; bounded frontier output path; memo diagnostic_snapshot | T3/T4 retained ownership and snapshot coverage |
| PARO_COMPILE_WORK_EVIDENCE, PARO_STATEMENT_CACHE_EVIDENCE, PARO_COLD_WORK_EVIDENCE | NormalEvidence; fixed counters/cache/first-execution ledger; context/session/runtime | preserve trace-off receipts until equivalent channel and timing boundaries verified |
| PARO_QUALITY_POLICY_HANDOFF, PARO_QUALITY_PREFLIGHT | BehaviorExperiment; quality handoff/producer-preflight behavior; planner/quality | C3 only; defaults unchanged |
| PARO_CERTIFIED_GROUP_PRUNING, PARO_DISABLE_PROTECTED_INCUMBENT | BehaviorExperiment; pruning/protected baseline; planner | C3 only, not output retirement |
| PARO_STRONG_INCUMBENT_EXPERIMENT, PARO_STRONG_INCUMBENT_PROVIDE_BOUND, PARO_STRONG_INCUMBENT_INJECT_LOGICAL | BehaviorExperiment; seed/bound/logical injection; optimizer | C3-M explicit replay/coverage gates |
| PARO_EXPORT_STRONG_INCUMBENT | Mixed; constructs export seeds, not just printing; planner | retain until export ownership/cost and replay dependencies reviewed |
| PARO_DIAGNOSTIC_OBLIGATION_ONLY | BehaviorExperiment; optional-lane agenda fallback; engine | C3 only |
| PARO_DIAGNOSTIC_SEARCH_STOP_MS, PARO_DIAGNOSTIC_FRONTIER_WIDTH | BehaviorExperiment; optional deadline/frontier capacity; budget | C3 only; never activated by COMPILE |
| PARO_DIAGNOSTIC_CARDINALITY_AUDIT, PARO_DIAGNOSTIC_NORMALIZE_CTE_DOMAIN, PARO_DIAGNOSTIC_SHADOW_STRUCTURAL_QUALITY, PARO_DIAGNOSTIC_STRUCTURAL_QUALITY_EVIDENCE, PARO_DIAGNOSTIC_SKIP_MEMO_VERIFIERS, PARO_DIAGNOSTIC_STREAM_SEQUENTIAL, PARO_STRONG_INCUMBENT_EXPORT_CANDIDATE, PARO_DIAGNOSTIC_REPLAY_CANDIDATE, PARO_DIAGNOSTIC_REPLAY_GRANT_CLASS | Mixed/Unclassified here: observed registry includes historical experiment names; no deletion based on prefix or absence of direct reads | retain manifest identity; historical lineage owner must close each dependency in T4/C3 |

All absent environment values remain absent; enabled/value interpretation stays
at the original reader (some test equality with `1`, others presence). SQL
`optimizer_verify`, profiling and resources remain production settings, not
COMPILE hints. Existing harness passes these through its observed-environment
contract; Summary neither sets them nor marks their absence as retired.

The machine-readable [control inventory](compile-observation-controls.json)
records each registry name, exact reader context, owner path, effective-value
interpretation, consumer and cache/manifest boundary. Registry-only historical
names are explicitly unresolved and protected, not silently classified as dead.
Process behavior controls are not independently included in the current plan
cache key; changing them within a live process is not certified. COMPILE does
not change them. The inventory is not a deletion allowlist.

T0 status: recoverable isolated baseline and per-entry classification complete;
main-tree hunk integration is not performed without an ownership decision.
C2's eight regress contracts and F2's independent evidence gaps remain open.
