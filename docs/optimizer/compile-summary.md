# Bounded compile Summary and execution association (T2/T4)

This is the bounded compile/execution contract, not C2/F2 admission or a
latency claim. Existing search, grant, verification and behavior-experiment
defaults are unchanged. Bounded Detail is delivered for the supported SQL
COMPILE shapes; full Trace Matrix coverage is not claimed.
The authoritative wire document is `context::compile_diagnostics::CompileDocument`;
the renderer and validator share its Summary/Unavailable variants and schema
version 2. Version 1 artifacts remain historical evidence, not current input.

## SQL and support boundary

```sql
EXPLAIN (COMPILE) SELECT 1;
EXPLAIN (COMPILE, FORMAT JSON) WITH t AS (SELECT 1 AS x) SELECT x FROM t;
```

Both produce one `QUERY PLAN` text column and one diagnostic document row.
Simple-protocol queries/CTEs use the production compiler exactly once on the
bare target AST. They bypass target cache lookup and publication (`ForcedCompile`,
not a cache-miss claim). `ANALYZE` admits and executes the same sealed compiled
artifact exactly once; it does not compile a second time. Extended Parse/Bind/
Describe/Execute supports known parameter types, does not execute on Describe,
and rejects incomplete parameter environments rather than inventing values.

| Combination | Status |
| --- | --- |
| simple query/CTE Summary, TEXT/JSON | implemented |
| binding/compilation error | original SQLSTATE and primary error preserved |
| cancellation | compiler and backpressured Summary delivery observe statement cancellation |
| DETAIL | bounded real Memo/TaskRegistry lifecycle Detail for supported SELECT/CTE COMPILE shapes; omitted records are explicit |
| ANALYZE | one actual admission and one execution of the sealed artifact |
| extended/prepared COMPILE | known-typed Parse/Bind/Describe/Execute supported; Describe does not execute |
| DML/DDL/utility/nested EXPLAIN | explicitly Unsupported |
| legacy EXPLAIN syntax | unchanged |
| source-build/catalog receipt and parse time | Uncovered, never guessed |
| actual admission/execution | attached only by the real ANALYZE execution receipt; otherwise NotExecuted |
| response-terminal measurement | Uncovered until the response boundary (T2) |

Unknown options, duplicates, conflicting FORMAT options, legacy option mixing
and trailing FORMAT in the new syntax are rejected. Target output identity is
hashed separately from the diagnostic result schema; literals/names are not
exported. Hash encoding 1 is a diagnostic identity, not semantic equivalence or
a cross-build receipt certification. Missing source/catalog receipts prevent
this Summary alone from certifying a cross-run experiment.

## Ownership and capacity

Capture ownership flows through StatementOptions, the compiled snapshot, the
result vector lifetime owner and the protocol sink. No TLS current-capture,
Memo snapshot, global event history or alternate planner is introduced.
Off creates no capture or buffer and performs no per-event environment lookup.

The retained profile is 1 MiB, encoded UTF-8 profile 200,000 bytes, at most
64 rule summaries and 16 portfolio summaries. Each live capture/result reserves
2 MiB including encoder/vector/wire-copy headroom. At most eight combined live
captures/retained results are allowed (stricter than eight of each); this is
below the 64 MiB process ceiling. Header and terminal fields fit within the
4 KiB reserved headroom. Summary exports no symbols or events.

Admission precedes allocation. Counters saturate on overflow, IDs do not wrap,
rule selection is bounded and deterministic, and whole optional records are
omitted rather than byte-truncated. Encoder failure emits a fixed bounded
Unavailable envelope. At process capacity the bare target still compiles once:
its original error wins; a successful target is reported as such in a bounded
diagnostic-capacity error detail, without allocating another retained document.
This is diagnostic request failure, not target compilation failure.

CollectingSink preserves the reservation across its copy. Pgwire transfers the
reservation to the `PgCodec` that owns the Framed connection as soon as the
encoded diagnostic bytes enter its write buffer. The codec releases it only
after that buffer is empty, or when the connection/codec is actually dropped;
canceling the send future alone cannot make pending bytes look free. A failed
flush therefore keeps the lease until the failed connection is discarded, and
does not require an unbounded retry flush. Sinks without this ownership
contract reject diagnostic delivery explicitly. Dropping a result, compilation
error or canceled request returns its reservation; no fallback file or
unaccounted queue is used. The snapshot is sealed before rendering.

Producers can mutate only fixed-size `CompileFields`; bounded methods own rule
and variant storage and the capacity profile is not exposed to mutation. The
renderer accepts only `SealedCompileCapture`. Its shared lease pins the small
record/reservation, never Memo or a plan. An automatic transaction started by
the request has a scoped rollback guard: errors, cancellation, unwinding and
future drop cannot attach it to the next statement. Explicit caller-owned
transactions remain under the ordinary statement/transaction error contract.
ProtocolSink records diagnostic write/flush failure through the same terminal
transport state as ordinary rows; no diagnostic-only retry/error-response path.

## Accounting and machine reading

Bind, optimize, verify, finish and compiler-other are disjoint intervals within
ParsedAstCompilerEntryToReturnV1. Their integer nanosecond sum must equal compiler
wall time. Parse is outside that boundary. Rule time is a separate projection
of optimizer time and MUST NOT be added to compiler phases.

Rule rows cover the union of binding time, apply attempts and publications.
`binding_calls`/`binding_ns` distinguish matching-only work; `attempts` counts
apply admission. Apply time is total `elapsed_ns` minus `binding_ns`. A no-match
or pre-apply budget refusal can legitimately have a row with zero attempts.
Additional binding counters are collected only for a compile capture.

Search completion, unresolved obligations, quality satisfaction and budget
status are independent fields copied from existing authorities. A verified
compiled portfolio is not an execution image and does not prove search closure.
`selected_fingerprint` is present only when the expected grant has exactly one
matching compiled variant; actual admission is not inferred.

```sh
cargo run -p paro-execution --example validate_compile_record -- capture.json
```

The reader bounds bytes before deserialization, rejects unknown schema/fields,
invalid capacity profiles, fabricated execution, unsupported completion claims
and non-closing phase sums. It validates a record, not cross-run association or
the correctness of an external arbitrary fingerprint.

## Artifact, admission and benchmark receipt contract

`CompileRecord.artifact_identity` is a versioned cross-run identity containing
artifact, structure and dependency words. It is emitted when the immutable
compiled artifact is ready. A portfolio summary is not an executable image.
The execution receipt is created at real admission and records expected class,
actual class/fingerprint, fallback reason and the resource contract. The
receipt moves through `image=NotReady` to `image=Ready` only after executable
lowering succeeds, then closes as Completed, Failed, Cancelled or Dropped.
Non-ANALYZE COMPILE cannot manufacture an admission receipt.

Normal benchmark execution reads the bounded `paro_optimizers()` channel after
each timed statement. It snapshots execution identities at the statement
boundary and associates exactly one newly completed execution receipt with
the matching immutable artifact and original compile receipt; it never chooses
the latest receipt or a maximum occurrence. Cache hits point to the original
compile receipt while the current compilation is NotExecuted. Association
failure is an explicit `Uncovered` reason and does not remove a timing or slow
sample. The run-owned output contract is documented in
[`compile-artifact-execution-receipts.md`](compile-artifact-execution-receipts.md).

## Historical T1 validation status (before independent review)

The following e1a04282-era results did not test transaction state after a
dropped request, unavailable-document reading, contradictory terminal states,
or the public mutation capacity bypass. They must not be read as certification
of those contracts. See `compile-summary-review.md` for the repair and new gates.

Tests: `session/tests/explain_compile_test.rs`, session's compile-request unit
test, context capacity/ownership/seal test, execution renderer/validator tests.
They cover query/CTE, malformed options, wide/deep input, non-execution, original
errors even at capacity, retained-result capacity, cache bypass, cancellation,
artifact neutrality and bounded encoding. Raw logs are retained under the T0
private recovery/evidence root recorded in `compile-observation-baseline.md`.

T0 is integrated into re-op with the user's mixed delta retained; see
`compile-integration-review.json`. The following paragraphs record the
historical T1/T2 checkpoint and are not the current support boundary. The
current bounded Detail and Summary-level T4 status is maintained in
[`compile-trace-matrix-status.md`](compile-trace-matrix-status.md).

Final clean source `d996be77` and its dev server binary are recorded in
[`compile-summary-validation.json`](compile-summary-validation.json):

| Gate | Result |
| --- | --- |
| workspace check / strict all-target Clippy | pass |
| workspace tests | 6859 pass, 0 fail, 85 pre-existing ignored |
| benchmark tests | 187 pass; one pre-existing pytest warning |
| regress harness tests | 101 pass, one pre-existing skipped |
| real pgwire query/CTE TEXT/JSON golden + schema reader | pass |
| cancellation, failed writer, dropped/backpressured request, retained capacity | pass |
| target non-execution, original errors, cache bypass, physical artifact neutrality | pass |
| full SQL regress, fresh test directory, verify on | 177 pass, eight existing failures |

All eight final `.actual` files are byte-identical to the preserved pre-T1 run
(25 unresolved EXPLAIN blocks). They remain failures, not blessed expectations.
A rerun in the already-used regression data directory produced a ninth failure:
`prepared_cursor_t` contained each fixture value twice and EXECUTE reported six
rows instead of three. That run/data/actual is retained; the final fresh-directory
run of the same binary has no such failure. Initial Python import-context and
server-readiness failures are also retained, not counted as passing attempts.

No performance/observer-overhead campaign was run. Functional artifact
neutrality is not timing neutrality near a deadline. No latency, parity, C2/F2
admission or complete Matrix claim is made. The full 99-query corpus was not
rerun for that request-observation change. The historical checkpoint did not
include T3 Detail; the current bounded Detail delivery and unaccepted legacy
DiagnosticOutput/BehaviorExperiment retirement are tracked separately, and no
behavior experiment was removed.

## Current T2/Summary-T4 validation boundary

The historical delivery below was validated from a clean release build after
the T2 artifact/receipt, bounded Detail, and Summary-level benchmark changes.
The current re-op branch has since added typed quality snapshots, canonical
physical identity, normal successful extended-Sync draining, and durable
RunOutput publication. Those changes have their own targeted checks; they do
not retroactively turn this historical snapshot into a full campaign result.
Workspace check and strict all-target Clippy pass; the benchmark suite passes
in the pinned DuckDB 1.5.5 environment, including exclusive RunId/attempt
ownership, retry retention, receipt identity matching, cache-hit NotExecuted
state, and the bounded manifest/writer path.

The fresh-directory SQL regression run reports 177 passed and eight existing
EXPLAIN-only differences. The eight are retained as failures and classified as
plan/display contracts: `agg_join_subsumption`, `agg_singleton_groups`,
`explain_analyze`, `explain_basic`, `join_explain_advanced`,
`rowset_scan_pushdown`, `statistics_query`, and `pgvector_topn_filter_flow`.
There were no result mismatches and no expected or `.actual` files were
blessed. A reused regression directory also produced a duplicate-fixture
`prepared_cursor_t` failure; it is retained as invalid-run evidence and is not
part of the fresh-directory result.

This validation covers real PgWire TEXT/JSON COMPILE, known-typed extended
parameters, one-shot ANALYZE execution, bounded Detail, receipt publication,
and cancellation/transport ownership tests. It does not certify full Trace
Matrix coverage, C2, F2, latency, or a 99-query performance campaign.
