# Optimizer convergence execution record

This record implements the dependency order of `optimizer-convergence-design.md`
and `optimizer-trace-matrix.md` r3 (2026-09-20). It is an in-progress record,
not ArchitectureReady, CompilerTargetMet or FirstStatementParity certification.

## Source and preservation

The main repository starts at `801cada4144a37645dfd44c5fe8c557579f7077b`,
branch `re-op`; the documentation repository starts at
`93e87982a754aee835d87514e46a0e3322d40b49`. Neither original worktree is edited.
The integration branch is `codex/optimizer-convergence-20260920` in
`/private/tmp/paro-convergence-20260920`.

Private backups are in
`/Users/linjunhong/paro-convergence-archive/20260920/c0/{main,docs,experiment}`.
Each contains separate staged/unstaged binary patches, the original index,
untracked file list and archive, HEAD and original status. The staged patches
are empty at capture; this is recorded rather than losing the distinction.
Restore verification applies both patches to a temporary Git index and checks
every restored file against the original. Untracked archive contents and
macOS metadata are checked separately. Inventories:

- [main](c0/main-inventory.json): 133 changed/untracked files verified;
- [docs](c0/docs-inventory.json): 17 verified;
- [experiment](c0/experiment-inventory.json): 57 verified.

Existing changes are owned by the prior work and remain unintegrated unless
individual hunks are reviewed. There is no whole-tree snapshot commit. No
evidence, data seed, worktree, build directory or Git history has been deleted.

## Finite delivery list

| Task | Status / next evidence |
| --- | --- |
| C0-a preservation and isolation | Restore verified; clean integration baseline created; fixture-repaired baseline 1319 pass / 15 fail |
| C0-b F2 attribution | Original binary and clean parent/probe Q39 repeated; historical dirty build inputs still missing; F2 isolated, not admitted |
| C2-1 Q39 | Full rows captured; independent integer-moment/80-digit oracle under investigation |
| C2-2 partial feasible grants | Engine/admission slice committed; [contract and tests](c2-partial-grants.md); SQL coverage outstanding |
| C2-3 facet ownership | Declaration deduplication/rollback and RF owner identity fixed; Q01/Q23 exact SQL match; Q02 internal error gone but derived-name schema mismatch remains |
| C2-4 failure adjudication | Four fixture cases adjudicated; implementation-phase omission fixed; optimizer suite now 1345 pass / 0 fail |
| C1 Trace Matrix | Pending; existing evidence interfaces suffice for C0/C2 |
| C3 + C3-M | Pending C2 and registered joint admission experiment |
| C4 | Workspace check and tests pass; strict clippy, SQL/harness and joint/performance certification outstanding |
| C5 | Inventory started; no deletion approved by a verified recovery package yet |

## C0-b: original F2 evidence qualification

The original `control.json` is a **single F2-after** broad report, not an
eager/lazy A/B. Its build manifest records `93518a81` plus dirty patch digest
`cd8a7c0eee482384542c2b8a48758c5f08599919647acb26c3a56a2cd0bceb11`.
It did not archive that complete patch or per-build-input contents. The
current residual experiment patch is preserved, but its identity must not be
silently substituted for the historical patch. The original binary is still
available and its SHA-256 exactly matches the report:
`f8bac8a71f40e588abba6175c38833daaf4a905dd25cf6ead04b76e9f0f7ca8e`.

The minimal diagnostic uses that binary, the same snapshot and DuckDB 1.5.5,
4 threads, 2 GB, handoff on and verify off, with a fresh private data copy and
server-observed environment validation. All result sets, ordered row values,
native types and binary float representations are retained. It produces no
performance claim. It does not weaken the original exact comparator.

Original-binary Q39 repeat: 243 rows on each engine; exact comparison reports
56 missing/unexpected rows. Key columns, row ordering and means agree.
All 57 unequal scalar values are in the two coefficient-of-variation columns
and differ by exactly one binary64 ULP. This is a repeat of the failure class,
not a claim that the historical count 55 was byte-identically reproduced.

An independent diagnostic obtains count, integer sum and integer sum-of-squares
for all 90,000 input groups. Paro and DuckDB integer values agree exactly.
Schema differences (`numeric` vs `HUGEINT` for sums) remain explicitly reported.
An 80-decimal-digit oracle computes
`sqrt((sum_sq - sum*sum/n)/(n-1))/(sum/n)`, without either engine's stddev.
For the 972 selected mean/covariance values, Paro differs from correctly rounded
oracle values by 0/1/2 ULP in 837/134/1 cases; DuckDB by 0/1 ULP in 839/133 cases.
These are diagnostic observations; no tolerance or result baseline was changed.

Raw files are retained in the private archive as `q39-original-binary-r1.json`,
`q39-integer-moments.json` and their server logs. The generator SQL is
[q39-integer-moments.sql](c0/q39-integer-moments.sql).

Clean parent `22d39fda` repeats return 243 rows and exact-float mismatches
of 57 and 62 rows. Clean probe `d3097038` repeats return 243 rows and mismatches
of 50 and 57 rows. They are not byte-identical failures: all per-value differences
and their source binary hashes are retained in [independent analysis](c0/q39-oracle-analysis.json).
All five captures (including original binary) have identical ordered integer
keys and the same 90,000-group integer-moment reference. In each capture the
selected Paro mean/CV values are at most 2 ULP from that reference; DuckDB at
most 1 ULP. Thus bit-exact equality is not a stable oracle for this floating
aggregate across these engines. This does **not** bless Q39: the benchmark
still rejects the results; a separately justified numerical result contract
and its integration tests remain required. No blanket tolerance is added.

The reproducible analyzer has four self-tests: independent duplicate-bag
sample variance, explicit 1-ULP reporting, integer/key/multiplicity loss, and
unsupported zero/NULL cases. It refuses incomplete column-role mappings.
A clean pair alone cannot repair missing historical source attestation; it is
separately identified evidence, and F2 remains isolated.

## Baseline failures and policy conflicts

The clean main-source optimizer suite initially does not compile: two fixtures
call `settle_arena_in` without resident identity; one uses the superseded tuple
form of `StagingInput::Native`; one omits `resident_nodes`. The isolated fix
uses the existing test identity helper and empty resident-node maps. It changes
no assertion and independently reproduces the matching fixture hunks already
present in the user's main worktree. Other mixed hunks are not imported.

Historical demands for equal fingerprints apply only to no-op changes. The r3
plan comparison contract governs intended semantic/estimation changes. Existing
SQL safety, exact child choices, unknown facts and resource feasibility remain
mandatory. Historical red tests require current causal adjudication; neither
the old “16 retained” list nor runner `new=0` is a baseline-pass certificate.

## Current release status

### C2 implemented slices and unresolved SQL gates

- [Partial grant coverage](c2-partial-grants.md) preserves feasible candidates
  and surfaces original implementation errors instead of claiming all classes
  must have mandatory winners.
- [Facet declarations](c2-facet-declarations.md) have one canonical contract,
  including full deferred declarations and rollback. The later Q01/Q23
  candidate-lifetime failure was traced to [RF owner identity](c2-rf-facet-owner.md):
  independent relation anchors were incorrectly combined into one facet.
  Owner isolation fixes these SQL cases without weakening the verifier.
- [Composite RF key identity](c2-composite-rf-identity.md) is independent of a
  single-column NDV lookup. Release Q05 on committed source `5d39d0b5`, with
  verifier enabled and observed-server configuration validated, returns 100
  rows with exact typed multiset equality. Full raw capture is retained as
  `c0/q05-composite-key-r1.json` in the private archive. This is correctness
  evidence, not a normal performance sample or a 20-query pass claim.
- [Implementation phase coverage](c2-implementation-phase.md) restores optional
  physical enumeration when no logical expression changed. The previous
  mandatory-only recipe prefix was incorrectly reused as completed optional
  search. The new regression and all original remaining assertions pass:
  **1344 optimizer tests passed, zero failed**. Restored legal work can alter
  plans and counts; this is not advertised as a speedup.

### Current clean release SQL retest

Source `2aabc2ede3538bcc9e4ae255b9ff411ec8a2b468`, release binary SHA-256
`521bb4c29d4fcd67686d45a58a1fda9e109971898bee5024327fdbb3d1dcfbe3`;
capture helper SHA-256
`fd42fd306741fc6a97d134087f8cb55910a4bb83c14c85ccac6ded44434b8a83`.
Seed, SQL, oracle and per-file harness hashes are in every raw capture.
Runs are serial, verifier on, four threads / 2 GB and server-observed settings
validated. These are correctness diagnostics, never compiler/C1 samples.

| Query | Rows | Original oracle result |
| --- | ---: | --- |
| Q01 | 100 | exact typed multiset match |
| Q05 | 100 | exact typed multiset match |
| Q23 | 4 | exact typed multiset match |
| Q02 | 2513 | still fails schema: `round(sat_sales1 / sat_sales2, 2)` versus `round((sat_sales1 / sat_sales2), 2)`; separately checked raw typed bag has zero missing/unexpected rows |
| Q39 | 243 | exact float comparator still fails, 54 missing/unexpected rows |

Raw archive names: `c0/q{01,02,05,23}-owner-r1.json` and
`c0/q39-integration-owner-r1.json`. The updated independent 80-digit integer
moment analysis is `c0/q39-integration-oracle.json`: 90,000 exact integer
groups, identical selected ordered keys; Paro 0/1/2-ULP counts 844/127/1 and
DuckDB 0/1-ULP counts 840/132. No comparator tolerance or schema exception is
introduced by this diagnostic. Q02 and Q39 remain failed gates.

Workspace `cargo check --workspace --locked` passes. The full workspace test
run reports 87 result summaries: **6843 passed, zero failed, 85 ignored**.
Logs: `c0/workspace-check.log`, `c0/workspace-tests.log`. Strict Clippy
(`cargo clippy --workspace --all-targets --locked -- -D warnings`) fails on
`context/statement_context.rs::capture` (too many arguments) and the
`storage/rowset/column/column_iterator.rs` test fixture (field reassignment
after Default). Log: `c0/workspace-clippy-strict.log`. The warning-tolerant
Clippy pass is not counted as passing this gate. Its remaining optimizer
warnings are also retained in `c0/workspace-clippy.log`; no lint suppression
or blanket automatic fix was applied.
SQL regress, 99-query certification and normal performance gates have not
been completed. Q39 still needs an admitted numerical oracle contract;
fixing three original internal errors does not certify the whole corpus.

F2 is not admitted to the integration baseline. The known historical failure
and its incomplete attribution remain open. No default policy, estimator,
normalization, resource grant or cost model has been changed in this stage.
Search-policy satisfaction and incomplete search remain distinct; there is no
ProofComplete, production readiness, sub-10ms or parity claim.
