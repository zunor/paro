# Python UDF SQL cases

These independently designed Paro cases exercise the real local Arrow Flight
service and isolated handler processes. They use ordinary SQL and reviewed
expected results, with no mock runtime or automatically accepted transcripts.

| Directory / case | Contract exercised |
| --- | --- |
| `types/integer_limits` | Signed and unsigned 8–128 bit extrema and NULL, in scalar and vector modes |
| `types/numeric_values` | Decimal(38,9), floating point magnitudes, booleans and NULL |
| `types/text_binary` | Empty and Unicode text, binary, JSON/JSONB and UUID round trips |
| `types/temporal_values` | Pre-epoch dates, leap days, microsecond time/timestamps, time zones and signed intervals |
| `types/nested_values` | LIST/ARRAY/STRUCT, fixed-width VECTOR, nested lists, empty containers, NULL parents and NULL children |
| `scalar/null_selection`, `semantics`, `context_and_branches` | Multi-argument STRICT compaction/scatter, called-on-NULL, Python-produced NULL, empty input, CASE branch selection, nested calls, zero arguments and immutable context |
| `scalar/arrow_kernels`, `arrow_capsule`, `fixed_width_fast_path` | Real Arrow compute, UTF-8, nullable arrays, sliced/chunked results, capsules and NumPy |
| `scalar/batch_boundaries` | Constant arguments and NULLs over 1/1023/1024/1025/4097 rows, checked against independently calculated counts/sums |
| `relational/values_boundary` | Large heterogeneous VALUES, bounded operator count, nested calls, unselected branches and nested NULL values |
| `relational/expression_positions` | WHERE, LEFT JOIN, GROUP BY/HAVING, aggregate arguments, window ordering, CTEs, scalar subqueries and DISTINCT |
| `transaction/dml_atomicity` | INSERT VALUES, UPDATE and DELETE, NULL filtering, rollback, and failure at row 3000 in UPDATE/INSERT SELECT/DELETE over 4097 rows |
| `table/`, `lateral/` | Empty/expanding/chunked tables, typed columns, output row limit, repeated outer parameters and NULL-preserving lateral joins |
| `ddl/`, `transaction/ddl_visibility`, `scalar/semantics` | Namespace and overload isolation, permissions, failed/rolled-back replacement, cross-session prepared-plan invalidation, drop/recreate and restart persistence |
| `errors/` | Invalid definitions, missing imports, handler exceptions, wrong result type/shape/schema, byte/row limits and successful calls after failure |
| `concurrency/independent_calls` | Concurrent sessions with distinct arguments and deterministic await order; fits the CI memory budget |
| `concurrency/timeout_recovery` | Five successive statement timeouts followed by native SQL and a successful UDF on the same connection |
| `availability/`, `explain/` | Disabled/missing/misconfigured runtime, degraded recovery, and external operator plans |

Expected results preserve SQLSTATE, types, row order and NULL distinctions.
The harness encodes an empty string as `(empty)`; an empty relation has a column
header and no data rows. New cases include teardown for interrupted assertions.
DML error cases verify autocommit statement atomicity; the harness rolls back a
connection after an expected error, so those cases do not assert failed explicit
transaction state.

Protocol cancellation, handler PID reaping, cancellation during admission,
owner/global queue bounds and service crashes are also tested at the Rust
server/client and Python worker entry points. SQL timeout tests complement those
checks; elapsed time and process identifiers are not unstable SQL baselines.
Malformed IPC declarations also have codec-level tests. SQL ARRAY syntax
denotes a variable-length array; VECTOR exercises fixed-size Arrow arrays.

Python aggregate/window **state handlers** are not supported; aggregate arguments
and window ordering may contain scalar Python calls. SQL package installation,
snapshot/tenant restore and MySQL zero-date semantics are not Paro contracts.
Dependencies belong to the selected interpreter environment.

Run `make python-udf-regress` against an owned local server. Availability and
persistence cases restart that server. See the root Makefile and
[regression guide](../../README.md) for compare-only operation and process scope.
