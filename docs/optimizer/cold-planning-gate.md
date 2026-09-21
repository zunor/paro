# Fresh-process cold planning gate

`benchmark/corpora/cold_planning.py` builds the measured server from the current
source, then starts an owned process and private copy of the supplied database
seed for every SQL file and process block. It never changes the SQL. The first
EXPLAIN follows only connection setup and SET statements. Startup, database
copying, connection and SET time are outside the timer. Filesystem caches are
not cleared: this measures a cold *process*, not cold disk I/O.

```sh
benchmark/.venv/bin/python benchmark/corpora/cold_planning.py \
  --server-data-dir /absolute/immutable/paro-seed \
  --query /absolute/original/11.sql --process-blocks 5 \
  --report benchmark/report/cold-q11.json
benchmark/.venv/bin/python benchmark/harness/cold_planning_gate.py \
  --report benchmark/report/cold-q11-run \
  --baseline benchmark/report/cold-q11-baseline-run
```

The collector's `--report` value is an allocation hint; the gate consumes the
sealed v3 RunOutput directory (`*-run`). It reads the manifest, the single
completed cell attempt, and its producer-owned cell payload. A free-standing
legacy report is not a current input.

The gate checks both median client EXPLAIN wall time and the optimizer phase
from the Rust-owned `EXPLAIN (COMPILE, DETAIL, FORMAT JSON)` document, plus
sampled peak process RSS. It rejects absent/failed blocks,
changed query coverage, invalid provenance, new rule failures, lost closure and
increased budget omissions. Group/expression and settlement counts are retained
for attribution, not forced to stay identical: removal of duplicate work is
allowed. The default latency/RSS tolerance is 15%; it is a regression alarm, not
a confidence claim or a substitute for execution comparison. Qualifying
comparisons require at least three independent process blocks. Allocation
instrumentation (`--alloc-metrics`) is a distinct measurement configuration.

The external wall/RSS watchdog can kill only the child it owns. This protects
the benchmark host even while testing broken cooperative optimizer checks.
Such samples fail; they are never counted as fast planning. Server logs, plans,
the typed compile document, binary/source/harness/data/SQL hashes and settings
are retained. `paro_optimizers()` is not a compile timing or search-counter
source for this gate; if used by an ordinary execution collector, it remains an
auxiliary receipt channel. RSS sampling has a 20 ms target interval (plus `ps` overhead), so it is
not an exact allocator peak. Execution correctness, cardinality q-error and
independent search-closure oracles remain separate required gates.
