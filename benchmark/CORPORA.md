# Optimizer correctness corpora

The optimizer correctness sequence uses external source repositories. Resolve
the selected Paro checkout from the user's task, not a fixed main-worktree
path. Set `PARO_WORKSPACE` to the verified directory containing the corpus
repositories below; an isolated Paro worktree need not share their parent.
Inspect source/status and use an owned test instance before building or loading
data. Preserve user changes and reuse agreed isolated checkouts.

| Corpus | Source repository | Query location |
| --- | --- | --- |
| JOB | `$PARO_WORKSPACE/duckdb` | `benchmark/imdb_plan_cost/queries` with answers in `benchmark/imdb/answers` |
| CEB | `$PARO_WORKSPACE/imdb-pg-dataset` | `ceb-imdb-3k` |
| CEB generation and metadata | `$PARO_WORKSPACE/ceb` | `templates` and `queries` |
| TPC-DS | `$PARO_WORKSPACE/duckdb` | `extension/tpcds/dsdgen/queries` (`01.sql`–`99.sql`, the `--query-dir` of `tpcds_compare.py`); `benchmark/tpcds` holds only `.benchmark` wrappers |
| TPC-H | `$PARO_WORKSPACE/duckdb` and this repository | DuckDB `extension/tpch/dbgen/queries`; Paro `benchmark/workloads/tpch` |
| LDBC SNB BI | `$PARO_WORKSPACE/duckdb` | `benchmark/ldbc/queries/bi-*.sql` |
| GRASP reference workloads | `$PARO_WORKSPACE/grasp` | `queries` |

Repository checkouts contain queries, schemas, generators, and expected
answers. Generated database files, exported CSV/Parquet data, server data
directories, logs, and comparison output stay outside every source repository.
Use an explicit data root such as `/tmp/paro-corpora` or another disposable
volume; never commit generated corpus data.

For the full correctness-corpus gate, run suites in this order (a targeted
diagnostic or refactor check need not claim or run this entire gate):

1. JOB
2. CEB
3. TPC-DS
4. TPC-H
5. LDBC SNB BI

Every Paro connection used by this gate enables `optimizer_verify`; record the
effective setting, rather than assuming a trace-off run has verification off.
A suite is complete only after each query executes and its decoded row multiset
matches the authoritative result. `EXPLAIN`-only success is not a correctness
result.

### JOB setup and exploratory baseline

`corpora/job_setup.py` reads the selected DuckDB checkout's 21-table schema and
official `duckdb-data` v1.0 Parquet URLs. Its explicit `download`, `export` and
`load` phases separate network preparation from CPU/I/O-heavy database imports.
The data root must be outside source repositories. A generated `dataset.json`
records source revision, SQL/answer hashes, release asset URLs/sizes/SHA-256,
exported row counts and the DuckDB database identity. Resume verifies existing
downloads against the current release identity and retained hashes. Downloads
use a durable owned pending journal and unique partial filename: a validated
partial or published file can recover an interruption before manifest commit.
An incomplete partial is discarded and fetched again; a final file without a
validated journal or committed asset record is rejected. Export requires every
pending asset to be committed. Export and load refuse to replace existing
database/table outputs.
No indexes or uniqueness declarations are added to either engine.

Run from `benchmark/` with `PYTHONPATH=.:corpora .venv/bin/python`:

```text
corpora/job_setup.py --duckdb-checkout <duckdb> --data-root <owned-data-root> download
corpora/job_setup.py --duckdb-checkout <duckdb> --data-root <owned-data-root> export
corpora/job_setup.py --duckdb-checkout <duckdb> --data-root <owned-data-root> load --dsn <owned-paro-dsn>
```

Load into an explicitly owned empty Paro instance started with its seed directory
as cwd and `--data-dir .`; stop it normally before comparison. Loading checks all
21 row counts. DuckDB runs ANALYZE during export; Paro uses storage-maintained
ingestion statistics because SQL ANALYZE is not implemented in this checkout.
The comparison records both statistics policies. Import failures leave their evidence/data in place
for diagnosis and do not authorize dropping tables or rewriting expected answers.
Only Paro's CREATE TABLE statements normalize the official plain column alias
`character varying(width)` to `varchar(width)`, preserving width, nullability,
and every other declaration. The DuckDB oracle uses the original schema. The
loader prints the source schema hash, normalized DDL hash, and transformation
description; `paro_ddl_inputs(checkout)` exposes those fields for an import
receipt without changing official input hashes or generated data.

`corpora/job_compare.py --help` exposes exact JOB query stems (for example
`--queries 01a,17b`), process-block and warmup/round counts, mandatory owned
listen/report/measurement-lock paths, and optional diagnostic compile plans.
It builds the tested checkout itself and uses immutable seed copies, fresh
DuckDB processes and the maintained TPC-DS timers/typed result contract. Every
sample's complete bag matches both the DuckDB oracle and pinned official answers
outside timing. Both engines have metadata-none inventories; all 21 row counts
and effective Paro `optimizer_verify` are checked before the query sequence.
Queries share each block's process; preflight and warmups are excluded from the
warm ABBA samples. These timings do not represent per-query cold starts or a
formal parity certification. Reports preserve failures, raw samples and process
blocks; `<report-stem>-run/` owns logs and optional diagnostic plans.
Pass `--import-receipt` to bind a completed post-shutdown importer record to
the exact dataset manifest, CSV identities, verified counts, normalized DDL
and immutable seed hash. A supplied mismatching record is rejected; absent
records are reported as uncovered import lineage. The importing binary may
differ from the tested binary, and seeds may be relocated without changing
their content identity.

For a paired TPC-DS SF1 correctness and latency comparison, use
`corpora/tpcds_compare.py`. The harness builds the tested Paro image itself and
owns every server process. Each block uses a fresh Paro server and a fresh
spawned DuckDB process. Within the block, configurable measurement rounds use
independently seeded-random ABBA order, so an isolated scheduling outlier cannot
stand in for a whole process while the process remains the outer resampling
unit. The harness applies the same thread and memory limits and validates the
exact result schema plus the complete row multiset outside every timed sample.
The report records source, build, corpus, DDL, database, optimizer-visible key
inventories, processes, round orders, and per-sample result digests.

Pass `--metadata-track none` for a qualifying engine comparison. The
`generator-declared` track remains useful for measuring Paro's best known plan,
but it is marked non-qualifying unless DuckDB exposes the same live key
inventory. Generated SF1 database and CSV files belong under the workspace data
root, outside source repositories.

For routine performance exploration, use
[paro-benchmark](../.agents/skills/paro-benchmark/SKILL.md). For a formal
performance claim, first read the repository's
[paro-evidence workflow](../.agents/skills/paro-evidence/SKILL.md), including its
comparison-validity contract.
Read the collector's live `--help` from the selected Python environment instead
of copying options from another worktree. Pin DuckDB's actual build and extension
identities as well as its version, and preregister the sample unit, thresholds
and resource envelope before confirmatory collection. Retain all valid slow
samples and explicit failures/exclusions. Target occurrence zero, cache state,
normal versus diagnostic cohorts and receipt association are distinct facts.
Check the chosen collector's actual RunOutput/receipt coverage; a typed schema
does not prove every statement/protocol has an observation. Do not infer
missing normal receipts from historical trace data. Archiving,
baseline updates and cleanup are not automatic parts of a comparison run.

## Declared competitor baseline

The declaration and the observation are separate evidence:

1. Read **this checkout's** [requirements.txt](requirements.txt), any actual
   referenced runtime/lock manifest, and the campaign's versioned registration.
   Record their source revision, content hashes and dirty state. A broad range
   such as `duckdb>=1.4,<2` is not an exact comparison baseline: resolve and
   approve an exact baseline before confirmatory sampling. An uncommitted pin
   is not automatically part of the committed source identity.
2. The package declaration constrains the DuckDB version. Binary/wheel, native
   `_duckdb` module, loaded DuckDB extensions, platform and settings need their
   own expected identities in the referenced runtime manifest/registration.
   Do not claim requirements alone specifies these hashes. Where no runtime
   manifest exists, register the approved artifacts before collection and mark
   absent evidence explicitly; do not cite an invented manifest filename.
3. Using the **selected interpreter and actual worker environment**, compare
   distribution/imported-package and native-engine versions, module paths and
   hashes, and the loaded extension set/configuration with that declaration.
   A controller's `pip show` or an observed hash by itself is not proof of what
   a worker loaded or of conformity to an approved baseline. Resolve differing
   declarations before running; do not simply choose the one matching the venv.
4. On mismatch or missing required identity, stop confirmatory collection and
   report the discrepancy. Do not silently update requirements, a venv, an
   extension or the registration to legitimize the installed version. An
   authorized baseline upgrade or explicit competitor-version experiment gets
   a new declared comparison; previous parity does not transfer to it.

The existing `tpcds_compare.py` report records an observed DuckDB version and
native Python-extension hash. Those fields are observations, **not** an already
implemented declaration-versus-runtime or loaded-extension-set gate. This
workflow requires the preflight; do not claim the current harness enforces it.

## Oracle-specific semantics

CEB targets PostgreSQL semantics. In particular, PostgreSQL widens
`REAL`/`NUMERIC` comparisons to double precision, while DuckDB narrows the
numeric operand to `FLOAT`. When DuckDB is used as the CEB execution oracle,
rewrite an explicit `value::float` operand to `value::float::double` on the
DuckDB side only. The Paro query remains unchanged.
