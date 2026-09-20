# Bounded compile Summary (T1)

This is compile observation, not C2/F2 admission or a latency claim. Existing
search, grant, verification and behavior-experiment defaults are unchanged.
The authoritative schema is `context::compile_diagnostics::CompileRecord`;
the renderer and validator share that Rust type and schema version 1.

## SQL and support boundary

```sql
EXPLAIN (COMPILE) SELECT 1;
EXPLAIN (COMPILE, FORMAT JSON) WITH t AS (SELECT 1 AS x) SELECT x FROM t;
```

Both produce one `QUERY PLAN` text column and one diagnostic document row.
Simple-protocol queries/CTEs use the production compiler exactly once on the
bare target AST. They bypass target cache lookup and publication (`ForcedCompile`,
not a cache-miss claim), discard the compiled portfolio, and never construct a
target execution image, admit a grant or execute the target.

| Combination | Status |
| --- | --- |
| simple query/CTE Summary, TEXT/JSON | implemented |
| binding/compilation error | original SQLSTATE and primary error preserved |
| cancellation | existing compiler cancellation, not a successful document |
| DETAIL / ANALYZE | parsed, explicitly Unsupported (T3/T2) |
| extended/prepared COMPILE | explicitly Unsupported at binder boundary (T2) |
| DML/DDL/utility/nested EXPLAIN | explicitly Unsupported |
| legacy EXPLAIN syntax | unchanged |
| source-build/catalog receipt and parse time | Uncovered, never guessed |
| actual admission/execution | NotExecuted, not zero time |
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

CollectingSink preserves the reservation across its copy. Pgwire holds it until
the diagnostic chunk is flushed. Sinks without this ownership contract reject
diagnostic delivery explicitly. Dropping a result, compilation error or canceled
request returns its reservation; no fallback file or unaccounted queue is used.
The snapshot is sealed before rendering.

## Accounting and machine reading

Bind, optimize, verify, finish and compiler-other are disjoint intervals within
ParsedAstCompilerEntryToReturnV1. Their integer nanosecond sum must equal compiler
wall time. Parse is outside that boundary. Rule time is a separate projection
of optimizer time and MUST NOT be added to compiler phases.

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

## Validation status

Tests: `session/tests/explain_compile_test.rs`, session's compile-request unit
test, context capacity/ownership/seal test, execution renderer/validator tests.
They cover query/CTE, malformed options, wide/deep input, non-execution, original
errors even at capacity, retained-result capacity, cache bypass, cancellation,
artifact neutrality and bounded encoding. Raw logs are retained under the T0
private recovery/evidence root recorded in `compile-observation-baseline.md`.

Final clean workspace, wire/backpressure and regress gates must be recorded
before claiming SummaryReady. T2–T5, legacy output retirement, C2's remaining
regress contracts and F2 admission are separate and remain unclaimed.
