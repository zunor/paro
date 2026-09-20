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

## Required cases

The real `PgWireResultSink`/`Framed<TcpStream, PgCodec>` tests cover:

- cancellation while the encoded diagnostic payload is backpressured;
- release only after a reader drains the pending bytes;
- two connections retaining independent capture capacity;
- release after the failed connection buffer and codec are dropped.

The existing protocol test continues to verify that a diagnostic flush failure
is terminal and cannot be followed by an ordinary error or completion write.
Session transaction rollback and sealed-document lifetime tests remain
unchanged.

## Validation boundary

The repair changes only transport ownership and observation. It does not alter
compiler search, grant selection, handoff, result comparison, SQL regress
expectations or execution semantics. Existing regress failures remain
unblessed and must be reported separately.

## 2026-09-21 validation

On clean source `7f0f5251`, the real Framed backpressure tests passed (three
new lease tests plus the existing result tests), the full `paro-server` test
target passed (51 library tests and 3 binary tests), the context capacity test
passed, the session compile lifecycle target passed (3 tests), and the
execution compile-render tests passed (2 tests). Workspace check and strict
all-target Clippy passed.

The required no-bless SQL regression run used a fresh data directory and
`ulimit -n 65536`: 177 passed, 8 failed, 0 skipped, and 0 new failures. The
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
