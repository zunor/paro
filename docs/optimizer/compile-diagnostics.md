# Compile diagnostics

## Public entry and meaning

```sql
EXPLAIN (COMPILE, FORMAT JSON) SELECT ...;
EXPLAIN (COMPILE, DETAIL, FORMAT JSON) SELECT ...;
EXPLAIN (COMPILE, ANALYZE, FORMAT JSON) SELECT ...;
```

The request observes the actual compiler once. Plain COMPILE forces compilation
without populating the ordinary statement cache; admission/execution are
`NotExecuted`. ANALYZE executes that artifact and attaches real receipts; its
side effects are real. TEXT and JSON render the same typed record.

The authority is [compile_diagnostics.rs](../../crates/context/src/compile_diagnostics.rs)
and its [work types](../../crates/context/src/compile_diagnostics/work.rs), currently
schema v4, with the maintained [consumer](../../benchmark/harness/receipt_contract.py).
Do not reconstruct fields from text logs or accept historical schemas by guesswork.

The four completed-stage observations are semantic normalization, regional
optimization, physical selection and physical construction. `Planned` means a
legal artifact, `PlannedWithFallback` a bounded regional fallback, not global
optimality. The exclusive work ledger includes unclassified work; it overlaps
stage durations and must not be added to them. Detail reports bounded stage
events and producer sequence; it is not the old per-candidate event stream.

## Identity, ownership and incomplete observations

Artifacts, statement decisions, admission and execution have separate typed
identities. Pair by explicit association, not latest row, occurrence or array
position. Compile ready does not imply admission, image construction or execution
completed. `Observed`, `NotExecuted`, `NotApplicable` and `Uncovered` differ;
absent data is not zero. Benchmark consumers accept only the explicitly accepted,
completed attempt of a cell; failed/cancelled/retried attempts remain visible.

Limits are owned by code, not duplicated formulas in architecture documents.
Capacity is reserved before capture allocation, snapshots seal immutably, and
omission/capacity outcomes survive as terminal observations. Encoding produces
valid bounded JSON, never a byte-cut document. Current defaults and hard limits
are constants in the producer; reports must state any changed observer settings.

The connection's codec owns the diagnostic lease while bytes remain in the
Framed buffer. A dropped sending future cannot release pending-byte ownership.
Draining or dropping the connection releases it. Cancellation drains the queued
prefix under the shared no-progress bound before the original ErrorResponse and
one ReadyForQuery; a stalled/force-closed connection terminates. Automatic
transactions recover, explicit transactions retain failure state. Do not create
a diagnostic-only transport queue or unbounded cleanup flush.

## Using it

Use COMPILE to locate planning work; use EXPLAIN ANALYZE for execution operators.
Normal latency samples remain trace-off and separate. Enabling the bounded
normal compile-work observer must be recorded and checked in an actual receipt.
Diagnostic timing cannot substitute for normal compile/C1 time. A different
diagnostic plan blocks joint attribution, not the valid normal measurement.

Capture, encoding, admission, deferred image construction and transport are real
first-statement costs. Moving them outside an optimizer timer does not remove
them. Use the selected checkout's actual supported statement/protocol paths;
an unsupported observer is a coverage gap, not proof the statement took no work.
