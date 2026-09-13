# Authorized prefix preparation assay — withdrawn, U-BATCH still incomplete

User approved prefix/publication-preserving microbatches. Probe e61fb27c used
lazy immutable child payload slots within a synchronous recipe visit, flat
storage and exact CandidateId-to-position lookup. No speculative pricing or
changed budget/publication/checkpoint order; preparation dropped on every return.
This is a **narrow preparation assay**, not batch budget/event registration.
The existing implementation already shares recipe read context per visit.

## Correctness and identity

Clean committed probe worktree:111 engine tests passed, including the independent
position/pruning test, budget retry, yielded-child consumption, nonselected-child
RF response and facts/grant/calibration/merge tests. Earlier dirty-tree tests
are separately retained, not substituted for the clean run. Full SQL regress
was not rerun this assay; prior164/20 and optimizer1213/5 failures remain open.

Fixed pre-registered C2→P2→P2→C2: control cd8020e1, probe e61fb27c, clean builds,
same original Q11/seed/model/budgets,4 workers/2GB, handoff1, normal trace-off/E1off,
compile scalars on, separate diagnostic/oracle/report. Typed ordered90 and
actual first-occurrence miss pass in all8 normal blocks. Inherited declared
Paro-key / empty DuckDB-key asymmetry remains disclosed and unchanged.

All normal synthesis counts1691. All292 final-winner fingerprint fields in each
diagnostic match exactly; admitted fingerprint remains
`5c29cf646706c8c8ba84000150211a6b`. No default switch or ProofComplete claim.

## Pilot result (all observations retained)

| Block-median metric | Control | Probe |
|---|---:|---:|
| Paro C1 |204.466ms|198.299ms|
| DuckDB C1 |123.382ms|106.683ms|
| Paro W |97.145ms|90.825ms|
| DuckDB W |111.499ms|103.992ms|
| Compiler |71.184ms|67.034ms|
| Optimizer minus rules |44.521ms|41.815ms|

Paired probe/control C1 geometric ratio .92076, simple four-pair bootstrap95%
interval [.80928,.99483]. This resampling does **not** remove temporal confounding:
DuckDB and untouched warm execution also speed up in probe batches. The valid
control263.7965ms sample remains. Neither the point estimate nor that interval
establishes a causal6ms improvement or formal W noninferiority.

Same-occurrence residual savings across pairs:1.917,11.825,3.494,.981ms;
mean4.55425ms. With the preregistered unchanged-intercept assumption the proxy
`20.4139 − mean_saved_us/1691` is17.72067µs, not≤12µs (required saving14.228ms).
This is not a new two-N marginal fit or measured synthesis-kernel time. The
intercept assumption is especially uncertain with observed machine drift.

## Disposition and remaining work

Withdrawn in ce28c8e0: production engine/tests restored byte-for-byte to control
for the touched paths. Do not retain another cache/preparation layer without
repeatable end-to-end benefit. Probe code remains inspectable in its commit.

**U-BATCH is not completed.** This assay neither implements N-unit prefix
registration nor falsifies the whole protocol hypothesis, and does not justify
"only domain identity/reducing N remains". The remaining authorized work is
actual prefix-aware budget/event batching with a flush before each observable
publication/cancellation/yield, preserving duplicate/retry identities and exact
admitted prefixes. No new permission is needed for that already-approved scope;
it was not implemented or measured in this assay. No claim of U≤12, parity,
default-policy improvement, or a formal noninferiority gate is made.

Raw reports bind full binary/source/harness/SQL/data hashes. Diagnostic times
are excluded from normal C1. `analyze.py` checks counts and winner fingerprints;
`raw/manifest.json` binds all raw and compressed bytes, including slow samples.
