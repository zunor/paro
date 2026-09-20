# C2 continuation: typed ORDER closed; production admission remains BLOCKED

This is a correctness record. No performance baseline was run. F2 remains
separately unadmitted; no chain/handoff default, search budget, cost constant,
grant policy or execution algorithm changed. User worktrees are preserved.

## 1. Typed ORDER contract

`3e6dbef7` implements [typed ORDER v4](typed-order-v4.md). Raw Decimal identity
remains exact; casts apply only to the already-bound arithmetic expression.
Paro and DuckDB 1.5.5 conversion chains are modeled separately, including
HUGEINT limb conversion. Scale >22/unproven conversion paths are still
Uncovered, not approximated by epsilon. Sixty actual-engine CAST pairs and
boundary/peer/LIMIT counterexamples passed. The parser-fixture scientific-text
failure is recorded in the contract note, not silently discarded.

[Both immutable 99-query screens](c2-v4-corpus-gate.json), rechecked on the same
v4 protocol: **each 98 exact + 1 independently bounded; zero Uncovered**.
Every identity/schema/own-wire/bag/order verdict and input hash is retained.
Q39's raw exact differences and existing independent certificate are unchanged.
These are certificates for the old captured binaries, **not** the new Rust fix.

## 2. TopN production path

`a5881e4d` restores the occurrence output contract before native TopN admission.
See [first-break proof and provider blocker](topn-output-contract.md).
The old failure was a generated product rejected before staging because it
exposed a hidden rank column; it was not cost rejection. The actual spill SQL
now selects/extracts/executes TOP_N_BUILD and TOP_N_EMIT and verifies results.
Only two new lineage fields were accepted in that transcript; no fields were
erased. The engine test separately checks publication and the selected output
contract, without forcing a VALUES plan winner.

The next link (native staging -> search provider) was tested and withdrawn.
The five-row [ranking counterexample](fulltext-rank-counterexample.sql) gives
scalar scores 2.375/2/2; the experimental indexed TopK chooses row 3 instead of
the unique scalar maximum row 1. Token-only and corpus-statistics BM25 are not
the same ordering contract. The rejected patch remains in the private archive
and applies to the accepted source (`git apply --check` passed), but is **not**
included in the production commit. No upper-bound witness or EXPLAIN selection
claim substitutes for execution equivalence.

## 3. Regress adjudication

`07262b32` declares the [session/settings envelope](c2-settings-adjudication-v4.md)
and retains all 28 process observations. Full verifier-on regress, fresh server,
FD 65536, four threads/2GB: **172 pass / 12 fail**, versus 166/18 previously.
Every remaining block and expected/actual hash is in
[the single-run block ledger](c2-v4-regress-blocks.json). All remaining differences
are EXPLAIN blocks; that does **not** certify the execution capability they promise.

Closed cases: `fulltext_rank_cd`, `explain_cardinality`,
`select_topn_fallback_spill`, `optimizer_observability`, `search_optimization`,
and `transaction_settings_savepoint`. The first, second, fourth and fifth
pass their unchanged expected output after the production fix. Only the spill
lineage additions and explicit settings contract changed expected text.

| Remaining case | Exact unresolved contract |
| --- | --- |
| fulltext_exec_mode_split, fulltext_index_coverage_guard, fulltext_pg | TopK provider coverage; connecting the native window exposes the ranking counterexample. Added scalar-rank execution assertion passes on accepted source; TopK EXPLAIN remains failed. |
| vector_search, pgvector_topn_filter_flow | Native provider publication is absent; TopN is now available, but provider versus ordinary scan legality/selection has not been certified. No vector expected text was blessed. |
| rowset_scan_pushdown | Blocks 17/33 still require access/late-materialization contract adjudication, not only row equality. |
| agg_join_subsumption, agg_singleton_groups | Cardinality/RF/artifact and aggregate placement differences remain unapproved. No assertion weakened. |
| explain_analyze, explain_basic, join_explain_advanced | JSON/schema/lineage and plan structure differ in the listed blocks; no bulk acceptance or field removal. |
| statistics_query | RF artifact identity change still needs its own producer/consumer proof. |

## 4. Clean integration and new-binary validation

Release source `a5881e4d` (later settings/docs do not change Rust), binary SHA-256
`ca1d32914fb3491883eba67e96d0922d994f13442a4115cb7c95e164cda1ef49`.
`cargo test --workspace`: **6847 pass / 0 fail / 85 existing ignored**.
Workspace all-target check and strict all-target Clippy (`-D warnings`) pass.
Benchmark/oracle suites: **196 pass**, one existing pytest return-value warning.
Regress harness: **101 pass / 1 existing skip**.

New-binary TPC-DS checks use the old screen's exact configuration (verify on,
handoff off, four threads/2GB), pinned DuckDB and private seed copy, with
server-observed environment validation. **Q04/Q11/Q47/Q57/Q74/Q89 all passed**
the complete v4 contract (6/90/100/100/92/100 rows respectively). Their captures
and identities are in [the validation manifest](c2-v4-validation-manifest.json).
Q11 was observed in a long
optional binding stage (`scoped_pattern_bindings` / `Enumerator::expression`),
but **completed with 90 exact rows, not a timeout**. The sample is diagnostic;
it neither quantifies a normal latency regression nor certifies performance.
These affected-query checks do not transfer the entire old screen's certificate
to a new binary. Full regress remains open.

The initial new-capture invocation incorrectly passed the integration harness,
which lacks the frozen lifecycle helper API; it failed before starting a server
or producing a capture. The retry uses the already registered frozen lifecycle
harness and current result contract. Two initial test commands used nonexistent
test directories; corrected commands above completed, and failed command logs
remain archived. None is counted as a test pass.

## 5. Gate status and stop

- ORDER/captured-corpus gate: closed for the attested 198 captures.
- Native TopN output and spill path: fixed and exercised, not a search-wide
  availability certificate.
- Fulltext/vector provider coverage, twelve regress failures and current-binary
  corpus admission: **NOT CLOSED**.
- Default-path performance baseline: **NOT RUN**, correctness gate is open.
- Full C2, F2 admission, ProofComplete, <10ms and parity: **not certified**.

No original captures, rejected experiments or user files were removed. The
remaining blockers have separate scopes; they must not be folded into a
successful numeric comparison certificate or renamed passes.
