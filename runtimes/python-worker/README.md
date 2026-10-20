# Python worker

Paro uses a local Arrow Flight service for Python routines. Rust embeds this
package and the small `paro_udf` context module into the server binary, extracts
them into an owned temporary directory, and starts the service lazily. A Python
interpreter with Python >=3.11 and PyArrow >=18 is required only for Python SQL.
`PARO_PYTHON_BIN` selects the interpreter; otherwise Paro uses `python3`.

The execution path is:

```text
native Paro vectors -> typed Arrow RecordBatch -> loopback Flight
    -> dedicated handler subprocess over Arrow IPC pipes
    -> typed Arrow result -> validated, accounted native Paro vectors
```

The Flight process never imports or executes a user module. DDL validates the
source digest, AST, synchronous handler signature and schemas without executing
module code. Validation shares Rust's owner/global admission gates, and the
service bounds concurrent AST/schema validation separately. Concurrent first-use
requests share one interpreter probe. Each invocation owns a handler process group. An independent
liveness watcher terminates that group if the service dies. Handler stdout and
stderr cannot corrupt IPC. This is process separation, not a security sandbox;
Python code runs with the server OS user's privileges. Packages must be installed
in that interpreter's environment. SQL does not install packages or import files
from staging URLs.

## Invocation contract

A service has a random token and epoch. Invocations use increasing epoch-scoped
ordinals, immutable routine IDs/generations and SHA-256 source digests. A bounded
ordinal window allows concurrent opens while rejecting replay even after old
terminal records are discarded. A started invocation is never automatically
retried. Replacing a routine creates a new catalog generation; running calls keep
their bound definition.

`DoExchange` carries a schema and sequenced Arrow batches. `consumed` confirms
input consumption; each `result` must be materialized and validated by Rust before
`ack` releases its one-batch result window. Client send-half-close marks EndInput.
The service emits `finish`, waits for the finish action, kills/reaps its handler,
and only then releases capacity. Cancel and Finish actions wait for that cleanup;
repeated terminal actions are idempotent while the record remains in the window.
Cancel arriving before admission records a terminal fence, preventing a late
open from starting user code. Rust assigns ordinals after local admission, so
queued calls do not age out of the replay window.
The synchronous Rust facade hands a Tokio worker back while waiting on Flight,
so protocol cancellation and other socket I/O remain runnable.
Rust currently submits one engine batch per invocation. The service also accepts
multiple sequenced batches with acknowledgement between results.

Cleanup failure retires the entire service epoch. An independent monitor also
retires the service if a cancelled or expired invocation cannot finish cleanup
within 10 seconds, including callbacks stuck in Flight input/output. Other calls
on that service fail; started calls are never replayed. Each handler supervisor
keeps the process leader's PID reserved until after killing the group, then reaps
it. Lease loss, termination signals and an independent lifetime deadline all
revoke the handler. A failed cancel acknowledgement causes Rust to stop the
service before releasing its local permit.

Limits are owned by `protocol.py`: 1 MiB controls, 16 MiB Arrow batches, 65,536
rows per batch, 8 live handlers, 4 per owner, and a 30-second invocation deadline.
Rust charges the transport working set to its admitted operator and bounds native
value materialization before encoding. The 68 MiB reservation includes wire
metadata; native result allocations are separately accounted. Rust rejects
compressed IPC and oversized declarations before Arrow decoding, and checks
nested native capacity before allocating vectors. Python sizes IPC streams before
allocating fixed output buffers. Handler processes also retire after 64
batches, 64 MiB of IPC, or 30 seconds. These transport limits are not an OS memory
limit for arbitrary Python allocations.

## Layout and tests

- `definition.py`: source and signature validation.
- `protocol.py`: version, bounds, schemas and control framing.
- `server.py`: Flight admission, sequencing, acknowledgement and terminal state.
- `process.py`, `watchdog.py`: owned processes and liveness.
- `handler.py`: scalar/vector/table invocation and Arrow IPC.
- Rust peer: `crates/external/src/python/`.

From the repository root:

```sh
python3 -m pip install -r runtimes/python-worker/requirements.txt 'numpy>=2'
make python-udf-unit
```

The tests use real Flight, real Arrow arrays and real subprocesses. SQL coverage
lives in `regress/cases/python_udf/`; `make python-udf-ci` additionally builds and
starts owned test servers (inspect its data/port settings before running).
