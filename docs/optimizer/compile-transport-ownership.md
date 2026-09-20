# EXPLAIN (COMPILE) transport ownership

This document records the transport-side completion of the bounded diagnostic
lease contract. It is separate from optimizer search policy and makes no C2,
F2, latency or parity claim.

## Ownership chain

The request owns the sealed `CompileCapture` while the compiler and encoder are
running. `PgWireResultSink` encodes the diagnostic row into the existing
`Framed` write buffer and transfers the same owner to the `PgCodec` attached to
that connection. The request future is then free to be canceled or dropped;
the connection codec, not the future, owns the reservation for bytes that are
still pending.

The codec keeps the owner until the transport observation sees an empty Framed
write buffer. This is deliberately conservative when unrelated protocol bytes
share the buffer: it cannot release a diagnostic lease while any buffered
output may still be pending. Dropping the connection drops the Framed codec
and its owners together, which is the discard path for a failed or closed
connection. A failed flush is not followed by an unbounded cleanup flush.

All existing result, extended-query, COPY and connection-level flush boundaries
observe the same codec-owned state. No diagnostic queue, TLS registry or second
transport protocol was introduced.

## Cancelled terminal output

The connection owns the cancelled simple-query epilogue. If statement
cancellation returns while output is already queued, it first drains that
prefix with the same no-progress policy used by COPY. A reduction in the
Framed buffer renews the 100 ms grace interval; a force-close or an interval
with no reduction abandons the connection and drops the codec. The connection
does not attempt an unbounded second flush.

Only after the prefix is empty does the connection feed the original
`ErrorResponse`. It drains that frame before returning `Sent`, allowing the
outer protocol loop to emit exactly one `ReadyForQuery`. A stalled or
force-closed terminal send returns `Terminate`, so no error or ready frame is
sent after a half-written result. The error object and SQLSTATE are preserved.

The pipeline boundary also restores an automatic transaction when a result or
transport error escapes while a sink is writing. This is a narrow fallback for
errors that bypass the statement-level rollback branches; it does not alter
explicit transaction failure semantics. Thus a resumed client observes the
cancel error followed by `ReadyForQuery('I')`, while an explicit transaction
remains failed rather than being silently committed or rolled back.

## Required cases

The real `PgWireResultSink`/`Framed<TcpStream, PgCodec>` tests cover:

- cancellation while the encoded diagnostic payload is backpressured;
- release only after a reader drains the pending bytes;
- two connections retaining independent capture capacity;
- release after the failed connection buffer and codec are dropped.
- a cancelled real `Connection`/`Session` query that drains the queued
  protocol prefix before one `ErrorResponse` and one `ReadyForQuery`;
- a force-close during that terminal send that resolves by dropping the
  connection rather than retrying the blocked buffer;
- automatic-transaction cleanup and original cancellation SQLSTATE on the
  resumed connection.

The existing protocol test continues to verify that a diagnostic flush failure
is terminal and cannot be followed by an ordinary error or completion write.
Session transaction rollback and sealed-document lifetime tests remain
unchanged.

## Validation boundary

The repair changes transport ownership, bounded terminal handling, and the
automatic-transaction fallback for sink errors. It does not alter compiler
search, grant selection, handoff, result comparison, SQL regress expectations,
or explicit-transaction semantics. Existing regress failures remain
unblessed and must be reported separately.

## 2026-09-21 validation

On clean source `76cc9e0c`, the real Framed lease tests, COPY cancellation
tests, and the two real connection cancellation tests passed. The full
`paro-server` target passed (53 library tests, 3 binary tests, and zero doc
tests); the context target passed (29 tests), the session compile lifecycle
target passed (3 tests), and the execution compile-render target passed (2
tests). Workspace check and strict all-target Clippy both passed.

The required no-bless SQL regression run for `5389032c` used a fresh data
directory and `ulimit -n 65536`: 177 passed, 8 failed, 0 skipped, and 0 new
failures. A second run on the amended source `76cc9e0c` produced the same
eight `.actual` files byte-for-byte. All eight are existing EXPLAIN text/JSON
or plan-identity differences; no SQL result mismatch was introduced. The
unresolved files are:

- `cases/query/aggregate/agg_join_subsumption.sql`
- `cases/query/aggregate/agg_singleton_groups.sql`
- `cases/query/explain/explain_analyze.sql`
- `cases/query/explain/explain_basic.sql`
- `cases/query/join/join_explain_advanced.sql`
- `cases/query/select/rowset_scan_pushdown.sql`
- `cases/system/statistics_query.sql`
- `cases/vector/pgvector_topn_filter_flow.sql`

Their current `.actual` files are byte-identical to the preserved post-review
baseline under `/private/tmp/paro-t1-review-DcAkmW/`; no expected result was
updated or blessed.
