# Node derivation and incremental publication

## Preregistered control and acceptance

Control: clean 47dc408e, corrected facts, admitted fingerprint
2ed1a3e2fe2f455d4411779edd54856b, 2500 syntheses, 1436 cumulative winners.
The old 1691/891 prefix is not a correctness target. User changes in the main
worktree are excluded. Implementations and evidence are committed separately.

Hypothesis: settled node results are discarded at the arena boundary and
staging rebuilds local child transports, layout and identity before publication.
Measure which assembly is actually redundant; fact merging after publication
is not presumed redundant. Keep CTE lexical producer order, exact boundaries,
partial aggregate namespaces, demand mappings, all selected proofs, and
transactional cancellation/rollback. Private settlement scalar/column/fact IDs
are not Memo IDs. Do not replace owned implementation capabilities with the
narrower native capability gate as a shortcut.

First compare fixed-input tests and deterministic Q11 final choices, costs,
per-rule publication and work counts. Diagnose relevant existing fact-cache,
CTE publication and nested RF failures; do not bless them. No budget, cost,
rule, frontier, default stop or quality-policy changes.

Performance runs are serial: original SQL and immutable SF1 seed, 4 threads,
2GB, fresh process/session, first target SELECT, cache miss, full typed 90-row
and peer-order validation. Normal trace-off pilots and opt-in exclusive B3
diagnostics remain separate. Use the existing harness and watchdog. Retain
all valid slow samples. Four normal blocks per arm are a pilot, not the
36-block formal W non-inferiority/parity campaign. Report per-occurrence
compiler, C1/W and diagnostic readiness/partition/work; no cross-cohort median
subtraction. Require reproducible compiler benefit with preserved execution
quality, otherwise remove unsupported optimization layers and record residual
work. QualityPolicySatisfied remains SearchIncomplete, not ProofComplete.

## Normal pilot order, declared before sampling

Probe (54bbe33a code) two blocks, control (3e4e6a51: 47dc408e code) four
blocks, probe two blocks. Each invocation retains the existing paired engine
order, one warmup and one measurement round (two W observations per block),
seed 2026091401, plus its separate diagnostic block. Four blocks per arm in
total; no sample-dependent extension. This brackets machine drift without
pooling normal and diagnostic samples. Compare all compiler samples and
engine-normalized paired C1/W; small-pilot uncertainty is explicit.

The 2d35b05b experiment is not admitted: suppressing the entire group-write
footprint changed the triple to 73a08efb/2203/1312. 54bbe33a retains the
structural publication dependency footprint while avoiding fact invalidation
on idempotent merges, restoring 2ed1a3e/2500/1436. The CTE cache correction
changes registry invalidations 97→93 in these diagnostics; final choices,
costs and all other fixed fields match control. No old statistics restored.

## Fixed pilot conclusion and final verification plan

The registered four-block probe did NOT lower compiler time: median 71.097ms
versus control 69.410ms. C1 did not improve either. Do not extend that sample
set or claim a speedup from the earlier diagnostic 107→92ms pair. The
copy/merge/compare publication layer (2d35b05b + 54bbe33a) is withdrawn; its
code and all negative evidence remain in history. The final code retains the
direct settled-node handoff and the independently demonstrated CTE producer
cache correctness repair, without a second fact merge layer.

After withdrawal, run one separate two-block fresh normal verification plus
its diagnostic cohort and the existing opt-in partition. This is a final-code
verification pilot, NOT an extension/replacement of the failed four-block
experiment and not evidence of formal non-inferiority. All three remaining
optimizer failures stay explicit; in particular RF survivor-domain identity
is still not a complete build-domain proof. No assertion is weakened to one RF.

## Outcome: partial contract consolidation, performance gate not established

Retained code: **f47543cf**, **d463749b**, related tests; final verification
source **79c00944**. The private settlement ColumnId/ScalarExprId/FactId catalog
is NOT treated as the Memo catalog. Conversion still occurs through typed
ColumnBinding and the existing Memo scalar importer, in the original order.
This round does not claim full session-wide scalar/identity unification.

### What was actually duplicated, and what was not

The arena already owns an immutable output layout for each generation. Old
staging instantiated completed child BoundReferences, assembled a local owned
parent, detached/assembled it again, then derived that output layout again.
Staging now consumes the resident layout and exact child NodeState directly;
the arena rejects stale/foreign generations. The old owned staging execution
branch was replaced, not retained as a second search implementation. Only a
new payload constructs its one-node cost/capability view. This deliberately
preserves the old singleton aggregate/window/inequality/RF admission semantics;
the narrower native capability gate is not substituted.

493 settled layout consumptions and 221 necessary cost views are observed in
the final diagnostic. These counts do not mean 272 whole derivations were
redundant: they include opaque operands and structural reuse. No complete Memo
or owned descendant tree is copied. Child fact handles survive publication
and are reused for the parent's cost view.

Settlement's local estimates and column values are NOT equivalent to Memo's
relational proof snapshot. The latter combines alternatives' source lineage,
CTE mappings, RF/control/replay evidence and equivalent fact merges. Those
operations remain. Staging still assembles Memo schema/column-domain/scalar
identities in its own namespace; the complete one-assembly target is **not
closed**. The retained change removes a concrete transport/layout round-trip,
not the entire 7ms settlement or 17ms staging bucket.

### Publication and invalidation findings

- A real red counterexample warmed a CTE reader, then changed only producer
  cardinality. Registry revision and reader-local facts stayed unchanged,
  but cached and uncached fingerprints diverged. The existing read cache now
  validates its exact ordered producer fact/statistic fingerprints as well as
  registry structure. Non-CTE reads remain independent. Producer proof updates,
  rollback, reinsert/merge and unchanged values are covered; no cross-query
  cache or second fact algorithm was added.
- Same-fact canonical merge correctly preserves the semantic fingerprint;
  the test now checks registry revision and cached/uncached agreement instead
  of demanding a fictitious value change. A separate changed-fact test remains
  mandatory, rather than weakening cache invalidation acceptance.
- 2d35b05b wrongly suppressed the entire publication write footprint for an
  idempotent merge. Q11 changed to 73a08efb/2203/1312. Retaining that footprint
  in 54bbe33a restored the original candidate, but its copy/merge/compare layer
  failed the registered compiler gate and was withdrawn (eb155462/b70a0b3a).
  Structural dependency publication and fact-value change still need a better
  separation; avoiding unchanged-fact notifications is **not completed**.

All retained diagnostics preserve 2ed1a3e2fe2f455d4411779edd54856b / 2500 / 1436,
exact final choices/costs and rule publication. Of 1627 fixed fields, the
producer-cache repair changes only task_registry_invalidation_count (97→93).
The full diff, including the rejected plan drift, is retained in summary.json.

### Normal measurements (not pooled with diagnostics)

| cohort | blocks | compiler median | Paro C1 / DuckDB C1 | Paro W / DuckDB W | C1 paired ratio [95% CI] |
|---|---:|---:|---:|---:|---|
| registered control | 4 | 69.410ms | 205.432 / 115.031ms | 96.149 / 107.684ms | 1.7302 [1.5727, 1.8416] |
| registered probe, publication layer included | 4 | 71.097ms | 231.443 / 123.107ms | 93.699 / 106.272ms | 1.9063 [1.7697, 2.0535] |
| final retained code, independent verification | 2 | 67.053ms | 200.513 / 107.226ms | 92.252 / 104.418ms | 1.8701 [1.8523, 1.8881] |

The registered experiment is negative: compiler +1.69ms, C1 +26.01ms. This is
not attributed wholesale to the changed code: cold residuals and machine
state were not causally isolated. It nevertheless fails the required evidence
of compiler benefit. Final verification's lower compiler point is encouraging
but two later blocks cannot replace the failed bracketed campaign or establish
a repeatable speedup. **No performance success, M1 or parity claim.**

Paro C1/W p95 respectively: control213.973/110.265ms;
registered probe247.146/96.785ms; final200.716/93.492ms. W paired ratios:
control .8250 [.6477,.9164], probe .8773 [.8634,.8884], final .8845 [.8768,.8926].
Control includes DuckDB W232.483ms and C1134.847ms, Paro W110.265ms; all are
retained. Ratios do not eliminate engine-specific machine noise. Four/two
blocks are not the 36-block formal W non-inferiority gate. T1 grant W NI
remains unclosed. No clear W deterioration is observed, not a formal NI proof.

All normal target occurrences are fresh, trace-off, cache misses, original
SF1 SQL, 4 threads/2GB, 90 typed rows with peer-order validation. Hashes for
source/binary/harness/SQL/data/resource declarations are in each raw report.
The exact image fingerprint is captured in separate same-build diagnostic
SELECTs, not independently emitted in normal cache evidence. Normal peak RSS
was not measured. These limitations are not hidden by an EXPLAIN or replay.
Default handoff/stop policy was not changed or benchmarked as a new default.

### Exclusive diagnostic ledger and remaining work

| B3 bucket | fresh control ON | final retained ON |
|---|---:|---:|
| native construction | 3.831ms | 3.512ms |
| statistics | 1.518ms | 1.336ms |
| owned rewrite | 0.734ms | 0.715ms |
| settlement | 7.314ms | 6.565ms |
| staging preparation | 8.313ms | 7.545ms |
| encoding/validation | 8.739ms | 7.763ms |
| guard / rollback / residual | .255 / .057 / .776ms | .251 / .053 / .762ms |

B3 coverage97.54% /97.33%; ledger sums equal their own optimizer totals.
OFF/ON/OFF optimizer: control99.775/99.778/90.374ms; retained88.820/88.089/
92.563ms. Bracketed ON perturbation +4.95% /−2.87%; the negative value is noise,
not an instrumentation speedup. Final qualified candidate65.911–69.750ms
in OFF diagnostics. These are not normal compiler or C1 samples. Counters and
per-rule unit costs are in summary.json; no nested times are added together.

Remaining measured work is still settlement plus staging, about21.87ms of
the final diagnostic B3, not the sub-millisecond owned rewrite. This round
does NOT isolate scalar/operator re-encoding versus relational proof merging
inside those buckets; it would be speculation to assign the remainder to one
of them. The next bounded design must separate structural publication
dependencies from fact-value invalidation and carry the already-derived node
contract into Memo lowering, without a copy/merge/compare optimization layer.

### Correctness and incomplete acceptance

Final clean optimizer **1310 passed /3 failed**. Arena7 pass, transformation
production tests (native CTE lexical domains, reordered mappings, partial
aggregate namespace and adopted proofs) remain covered. New tests verify
resident-layout generation safety, same payload/new NDV evidence, same-value
merge, producer-only fact changes and rollback. The retained implementation
does not change comparison, budget, cost constants, rule set, selected child
coverage or FrozenCandidate verification.

CTE multi-output fixture now declares the session's actual grant classes:
its two-output assertion passes. It permits exactly the non-expected classes'
OptionalGrantDeferred obligations, no budget/exhaustion/other obligation.
The historical allocation/publication defect is not reproduced by this fixture;
no production allocation patch or relaxed publication assertion was added.

Three failures remain, unblessed: nary-sharing and mark-to-semi fixtures still
reject an undeclared expected grant; nested RF retains two actual RF edges but
collapses two survivor domains because its proof key omits the distinct build
domain. That RF proof identity gap is relevant, **not closed by this task**,
and its expected retention count remains two. Full SQL regress was not run;
historical164/20 is not reported as a current result. Search remains
QualityPolicySatisfied + SearchIncomplete, not ProofComplete.

Recompute with `python analyze.py`; archive hashes are in raw/manifest.json.
The main user's mixed staged/unstaged edits are excluded from clean evidence.
