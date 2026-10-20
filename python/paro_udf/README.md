# Paro Python functions

Python >=3.11 and PyArrow >=18 are required. `paro_udf` provides an immutable
`Context` and a small local test helper. Vector arguments are actual
`pyarrow.Array` instances; there is no Paro column wrapper or simulated capsule.
NumPy is optional for handlers and required for the NumPy interoperability tests.

Inline SQL bodies receive their named arguments and `ctx`. Scalar mode is the
default and calls the handler once per row with Python scalar values:

```sql
CREATE FUNCTION add_one(a INTEGER) RETURNS INTEGER
LANGUAGE python STRICT
AS $$return a + 1$$;
```

`MODE VECTOR` calls once per batch and requires an Arrow array of exactly the
SQL return type and input row count. Arrow kernels may widen types, so explicitly
cast the result when needed:

```sql
CREATE FUNCTION add_seven(a INTEGER) RETURNS INTEGER
LANGUAGE python MODE VECTOR STRICT
AS $$
import pyarrow.compute as pc
return pc.add(a, 7).cast(a.type)
$$;
```

Use `HANDLER 'name'` for a full Python module instead of an inline body:

```sql
CREATE FUNCTION double_value(a INTEGER) RETURNS INTEGER
LANGUAGE python HANDLER 'calculate'
AS $$
def calculate(ctx, a):
    return a * 2
$$;
```

A handler must be a module-level synchronous function with exactly `ctx` plus
one positional argument per SQL parameter, without decorators, defaults,
variadic or keyword-only parameters. Module top-level code executes in the
handler subprocess, not during CREATE FUNCTION. No module state persists across
invocations. `IMMUTABLE`/`STABLE` remain declarations about SQL semantics.

`STRICT` removes rows with any NULL argument before invoking Python and scatters
scalar/vector results back with NULLs. An all-NULL batch does not start user code.
`CASE` invokes external branches only on their selected rows. Zero-argument
vector functions use `ctx.num_rows` to construct their result.

`RETURNS TABLE (...)` uses batch arguments and returns a `pyarrow.RecordBatch`
or `pyarrow.Table` with matching column names/types. It may change row count,
within the worker batch limit. Lateral table functions retain Paro's existing
correlation semantics. Python aggregate/window state handlers are not supported.

Types retain their exact width, scale and timezone. Lists, fixed-size arrays and
structs are recursive. UUID is fixed-size 16-byte binary; JSON is UTF-8 and is
validated on return. HUGEINT/UHUGEINT use Arrow Decimal256(39,0), with native
integer range checks on return. INTERVAL is a struct of `months: int32`,
`days: int32`, `micros: int64`, preserving the full microsecond range.

`Context` contains `num_rows`, `invocation_id`, `routine_id` and `generation`.
For local vector tests:

```python
import pyarrow as pa
from paro_udf.testing import vector_call

result = vector_call(lambda ctx, a: a, pa.array([1, None], type=pa.int32()),
                     return_type=pa.int32())
```

Install with `python3 -m pip install -e 'python/paro_udf[numpy]'`. Install external
libraries into the selected worker interpreter; SQL PACKAGES/IMPORTS and
CAPABILITY PROFILE overrides are rejected.
See the [worker contract](../../runtimes/python-worker/README.md) for limits,
process ownership, cancellation, and the absence of OS sandboxing.
