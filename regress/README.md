# Paro SQL Regression Suite

End-to-end SQL testing for Paro.  
Each test case is a plain `.sql` file paired with a `.result` baseline — no bespoke
test framework to learn, no YAML, no JSON fixtures.

```
regress/
├── cases/           # test cases organised by topic
│   ├── ddl/
│   ├── dml/
│   ├── errors/
│   ├── python_udf/
│   ├── types/
│   └── …
├── fixtures/        # staged test-only inputs for narrow cases such as python_udf
├── harness/         # parser, executor and comparator (pure Python)
├── unit/            # unit tests for the harness itself
├── report/          # generated after each run (gitignored)
├── runner.py        # standalone entry point
├── config.toml      # connection & test defaults
├── requirements.txt # Python dependencies for local runs and CI
└── Makefile         # everything you need
```

---

## Quick Start

```bash
# 1. Install dependencies (one-time)
make setup

# 2. Start the Paro server (in another terminal)
#    e.g. cargo run -p paro-server --bin parod

# 3. Run all regression cases
make check

# 4. Run a subset
make check FILE=query/select
```

> You can also invoke these from the **project root**:
> `make regress`, `make regress FILE=types`, etc.

Python UDF specific entry points also exist at the project root:

- `make python-udf-regress`
- `make python-udf-startup-smoke`
- `make python-udf-ci`

---

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Create `.venv` and install Python dependencies |
| `make check` | Run all regression cases |
| `make check FILE=<pattern>` | Run cases whose path contains *pattern* |
| `make update` | Regenerate **all** `.result` baselines |
| `make update FILE=<pattern>` | Regenerate baselines for matching cases only |
| `make check-parallel` | Run with `--jobs` (default 4) |
| `make ci` | CI mode — compare only, write `*.result.actual` |
| `make unit` | Run harness unit tests |
| `make ping` | Verify Paro server connectivity |
| `make clean` | Remove `.venv`, `__pycache__`, and reports |
| `make help` | Print all available targets |

### Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `PARO_HOST` | Override server host | `PARO_HOST=10.0.0.5` |
| `PARO_PORT` | Override server port | `PARO_PORT=5432` |
| `PARO_DATABASE` | Override database name | `PARO_DATABASE=mydb` |
| `PARO_USER` | Override database user | `PARO_USER=admin` |
| `PARO_PASSWORD` | Override database password | |
| `PARO_UPDATE` | Force update mode (`1`/`0`) | `PARO_UPDATE=1 make check` |
| `PARO_WRITE_ACTUAL` | Write `.actual` files on mismatch | `PARO_WRITE_ACTUAL=0` |

Configuration priority: **CLI flags > env vars > `config.toml`**

---

## Writing Test Cases

A test case is a `.sql` file under `cases/`.  The runner connects to Paro,
executes each SQL statement, captures the output (results or status messages), 
and compares it against the sibling `.result` file.

### minimalism

Test files are designed to be as close to raw SQL as possible. 
Directives are **optional** for standard queries and statements.

- **Auto-detection**: The harness automatically distinguishes between queries (SQL that returns rows like `SELECT`, `SHOW`, `EXPLAIN`) and statements (SQL that returns a status message like `CREATE`, `DROP`, `INSERT`).
- **Soft-fail**: If a query or statement fails, the harness captures the error message and treats it as the result for comparison. No special error directive is required unless you want to validate a specific error regex.
- **Clean Baselines**: `.result` files are transparent transcripts. Default directives (like `-- @query nosort` or `-- @statement ok`) are **ommitted** in the output to keep it clean.

### Directives

Directives are SQL comments that control the runner's behaviour.

| Directive | Purpose |
|-----------|---------|
| `-- @setup` | Executed once; errors here skip the case. **Mandatory** for setup blocks. |
| `-- @teardown` | Always executed at the end. **Mandatory** for teardown blocks. |
| `-- @query rowsort` | Sort rows before comparison. |
| `-- @query valuesort` | Sort individual values. |
| `-- @query hash` | Compare MD5 hash of output (for large result sets). |
| `-- @query approx(0.01)` | Floating-point comparison with epsilon. |
| `-- @query file` | Read a local file and compare its contents (SQL should be `FILE '<path>';` or a single-quoted path). |
| `-- @query json` | Parse JSON result cells and compare canonicalized structure instead of raw text. |
| `-- @statement ok` | Force a block to be treated as a statement (rarely needed). |
| `-- @statement error <pattern>` | Validate that an error matches a specific regex/SQLSTATE. |
| `-- @normalize <profiles>` | Apply registered text normalizers before comparison. Unknown profiles fail during parse. |
| `-- @control <action> [key=value …]` | Execute a harness-side control action such as `restart` or `connect` before the next SQL block. |
| `-- @session <name> [user=...] [database=...] [async=label]` | Execute the next SQL/query/copy block on a named persistent connection; `async` runs it in the background. |
| `-- @await <label> timeout=<duration>` | Wait for a background block and emit its output at this stable transcript position. |
| `-- @sleep <duration>` | Sleep briefly to let a known blocking point enqueue. |
| `-- @wait_expect interval=<duration> timeout=<duration>` | In check mode, repeat the next query until it matches the existing baseline or times out. |

SQLSTATE-sensitive cases should assert the exact SQLSTATE in the transcript or
include `SQLSTATE=<code>` in the `-- @statement error` pattern. Keep `XX000`
only for internal bugs/assertions; user-facing catalog, data, transaction,
resource, and external-routine errors should use their semantic SQLSTATE.

### Normalize Profiles

`@normalize` is intentionally narrow: it exists to mask volatility, not to hide
product-contract problems. Current registered profiles are:

| Profile | Purpose | Rewrites | Lifecycle |
|---------|---------|----------|-----------|
| `explain_operator_timing` | Normalize EXPLAIN ANALYZE per-operator timing text | `actual time=...` | Stable |
| `explain_summary_timing` | Normalize EXPLAIN summary timing text | `Planning Time: ...`, `Execution Time: ...` | Stable |
| `explain_runtime_bytes` | Normalize volatile spill/memory byte fields | `Memory: ...`, `Disk: ...`, `Peak Memory: ...`, `Temp Storage: ...` | Stable |
| `explain_routine_ids` | Normalize catalog ids embedded in routine labels | `Routine: name[id@generation]`, `Routines: ...` | Stable |
| `explain_external_runtime` | Normalize volatile external runtime latency fields | `Latency(us): acquire=... queue=... kernel=... encode_decode=...` | Stable |
| `explain_runtime` | Legacy alias combining operator + summary timing normalization | `actual time=...`, `Planning Time: ...`, `Execution Time: ...` | Transitional |
| `transaction_ids` | Normalize volatile ids in concurrency error text | `TxnId(...)`, `transaction ...`, table/db/read/commit ids | Stable |
| `python_runtime_retry_hint` | Normalize Python runtime degraded retry countdowns | `next automatic Python runtime probe in ... ms` | Stable |

Notes:

- `explain_runtime` is a temporary compatibility alias. New cases should prefer
  `explain_operator_timing` and `explain_summary_timing` directly.
- `explain_runtime_bytes` only masks volatility in byte counts. It must not be
  used to hide missing/incorrect spill fields.
- Normalizers must preserve line count, indentation, labels, and all
  non-target text.

Example:

```sql
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE SELECT * FROM t ORDER BY id;
```

### Control Blocks

`@control` is intentionally small and only exists for SQL E2E scenarios that need
runner-managed orchestration rather than product SQL syntax.

Current actions are:

| Action | Example | Purpose |
|--------|---------|---------|
| `restart` | `-- @control restart profile=python_disabled` | Restart the local `parod` listener with a named runtime profile from `config.toml`. |
| `connect` | `-- @control connect user=routine_builder` | Reconnect using a different startup identity or connection override. |

Notes:

- `restart` only works against a local Paro listener (`localhost` / `127.0.0.1`).
- Runtime profiles live under `[runtime_profiles.<name>]` in `regress/config.toml`.
- Profile-managed environment keys are cleared before each restart, so switching back to `profile=default` returns to a clean runtime state.
- `connect` preserves the current connection target unless you override fields such as `user`, `database`, `host`, `port`, or `password`.

### Multi-session Transaction Cases

Cases under `cases/txn/interleavings/` may use `@session` to express deterministic
interleavings with multiple persistent connections:

```sql
-- @session s1
BEGIN ISOLATION LEVEL SNAPSHOT;

-- @session s2 user=paro database=postgres
INSERT INTO t VALUES (2, 'new');

-- @session s1
SELECT * FROM t ORDER BY id;
```

Named sessions live for the duration of the case and keep transaction state
across blocks. Non-default session output is labeled in the transcript with
`-- session: <name>`. At case end, the harness fails if any session is still in
an open or failed transaction, then rolls back and closes all named sessions.
`@setup`, `@teardown`, and `@control` always run on the default session.

Blocking interleavings use `async=<label>` plus `@await`:

```sql
-- @session s1
BEGIN;

-- @session s2 async=blocked_update
UPDATE t SET v = 20 WHERE id = 1;

-- @sleep 100ms

-- @session s1
COMMIT;

-- @await blocked_update timeout=5s
```

Background output is written where `@await` appears, which keeps `.result`
files deterministic. If `@await` times out, the harness disconnects that named
session and fails the case. `@wait_expect` is reserved for bounded catch-up
polling; update mode executes the query once because there is no baseline to
poll against.

### Python UDF Fixtures

Python UDF SQL cases live under `cases/python_udf/` and mirror the long-term topic
split from the design docs:

- `ddl/`
- `scalar/`
- `table/`
- `aggregate/`
- `window/`
- `transaction/`
- `lateral/`
- `errors/`
- `explain/`
- `availability/`

Reusable Python modules, packages, and data belong under `fixtures/python_udf/`.
The fixture contract is intentionally narrow:

1. Declare staged roots with `-- @fixture python_udf/modules/basic_math`
2. Reference the staged location with `{{fixture:python_udf/modules/basic_math}}`
3. Keep artifact resolution and worker bootstrap on the real product path; fixture staging only copies files into the run-local report area

Example:

```sql
-- @fixture python_udf/modules/basic_math
-- @query file
FILE '{{fixture:python_udf/modules/basic_math}}/basic_math.py';
```

### Example

**`cases/example/quickstart.sql`** — the README showcase query with full setup:

```sql
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (
    id INT PRIMARY KEY,
    author_id BIGINT,
    title VARCHAR,
    body VARCHAR,
    embedding VECTOR(4)
);

INSERT INTO docs VALUES
    (1, 1, 'Building agent memory systems',
          'Designing vector retrieval for agent long-term memory',
          '[0.92, 0.11, 0.74, 0.33]');

-- ... tables, graph, and indexes omitted for brevity ...

WITH network AS (
    SELECT * FROM GRAPH_TABLE(social_graph
        MATCH (me:Person WHERE me.name = 'Ada')
              -[:Follows]->{1,2}(friend:Person)
        COLUMNS (friend.id AS author_id, friend.name AS author_name)
    )
)
SELECT
    d.title,
    n.author_name,
    1.0 / (1.0 + (d.embedding <-> '[0.91, 0.10, 0.80, 0.22]'))
      + ts_rank(
            to_tsvector('simple', d.body),
            plainto_tsquery('simple', 'agent retrieval')
        ) AS score
FROM network n
JOIN docs d ON d.author_id = n.author_id
WHERE to_tsvector('simple', d.body) @@ plainto_tsquery('simple', 'agent retrieval')
ORDER BY score DESC
LIMIT 10;

DROP PROPERTY GRAPH social_graph;
DROP TABLE follows;
DROP TABLE people;
DROP TABLE docs;
```

**`cases/example/quickstart.result`** (generated by `make update FILE=example`):

```
-- ... DDL status messages ...

SELECT ...
title	author_name	score
Agent orchestration patterns	Grace	0.9183340762495488
Multi-model retrieval	Margaret	0.917891101488498

DROP PROPERTY GRAPH social_graph;
DROP PROPERTY GRAPH

-- ... cleanup ...
```

### Generating Baselines

After writing a new `.sql` file, run:

```bash
make update FILE=query/select/basic_select
```

This executes the case against the running server and writes (or overwrites)
the `.result` file.  **Always review the generated baseline before committing.**

---

## How It Works

The harness consists of three stages wired together by `runner.py`:

```
  .sql file
     │
     ▼
  ┌──────────┐    Structured blocks (setup, query, teardown, …)
  │  Parser   │──▶
  └──────────┘
     │
     ▼
  ┌──────────┐    Query outputs (columns + rows per query)
  │ Executor  │──▶
  └──────────┘
     │
     ▼
  ┌───────────┐   PASS / FAIL / NEW / SKIP
  │Comparator │──▶
  └───────────┘
```

1. **Parser** (`harness/parser.py`) — Splits `.sql` files into directive
   blocks, handling dollar-quoting, multi-line statements, and comments.
2. **Executor** (`harness/executor.py`) — Runs each block against Paro via
   `psycopg`, collecting column headers and typed row data.
3. **Comparator** (`harness/comparator.py`) — Serialises outputs into the
   transcript format and diffs them against the `.result` baseline. Supports
   exact, sorted, hash, and approximate comparison modes.

---

## Reports

After every run, three files are generated in `report/`:

| File | Content |
|------|---------|
| `report.txt` | Per-file summary with timings and pass/fail counts |
| `error.txt` | Detailed diffs for every failed case (SQL, expected, actual) |
| `regress.log` | Full debug log |
| `actuals/` | Flattened `.actual` files for failed cases |

---

## CI Integration

```bash
make ci
```

This runs in **compare-only** mode (`PARO_UPDATE=0`) with `--verbose` and
writes `.actual` files for every mismatch.  The exit code is non-zero if any
case fails or is missing a baseline, making it suitable for use in CI pipelines.

---

## FAQ

**Q: Where does the `.venv` go?**  
Into `regress/.venv/`. It is gitignored and disposable (`make clean` removes it).

**Q: Can I use the system Python directly?**  
Yes. If no `.venv` exists, the Makefile falls back to `python3`. You can also
override it: `make check PYTHON_SYS=/usr/bin/python3.12`.

**Q: How do I add a new test category?**  
Create a new directory under `cases/` (e.g. `cases/vector/`), add `.sql` files,
run `make update FILE=vector`, and commit the generated `.result` files.

**Q: What happens if the `.result` file is missing?**  
The case is marked **NEW** and the run exits with code 1.
Run `make update` to generate the missing baselines.
