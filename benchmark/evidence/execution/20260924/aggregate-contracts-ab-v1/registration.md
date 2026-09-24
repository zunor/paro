# Aggregate contracts: fresh before/after comparison

EvidenceId: aggregate-contracts-ab-v1-20260924. Registered before collection.
This is a bounded engineering comparison, not a powered parity certification.
No observations from aggregate-contracts-v1 are reused as controls.

## Arms and claim

- Control: clean `e37ae93f`, immediately before the three implementation commits.
- Probe: clean `4fea3061` plus this registration only. Runtime changes are
  `0c58eb3d` (compact keys with state projection), `57a98fe2` (disjoint equality
  selectivity), and `09322560` (owned aggregate fragment merge).
- The intervention is the combined change. Costing can change plan/work;
  this experiment cannot isolate the two execution changes' individual effects.
  The aggregate placement quality policy is unchanged and still incomplete.

Use the maintained tpcds_compare.py from each selected clean source. Its source,
binary/build and harness identities are required, and harness source must match
between arms. One temporary control source worktree may share the main target
through an ignored symlink; all builds and measurement processes are serial.
Do not replace collector builds with unattested binaries. Preserve the existing
historical worktree and all user processes. Remove only the newly owned control
worktree after verifying that it has no changes.

## Fixed context

Q04, Q11, Q74 SQL from the local DuckDB TPC-DS extension. Pin SQL/CSV/schema and
query-corpus digests in each maintained collector's inputs manifest.

- Paro seed: `/private/tmp/paro-migration-relative.u1PLBV`, SHA256
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
- DuckDB SF1 database SHA256:
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`.
- Declared/runtime DuckDB 1.5.5, Python 3.14.3. Native module SHA256:
  `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
  Loaded core_functions/icu/json/parquet are built-in v1.5.5.
- Four threads, 2GB, binary results, quality search policy, optimizer_verify off.
  Set `PARO_COMPILE_WORK_EVIDENCE=1` and
  `PARO_DIAGNOSTIC_SEARCH_STOP_MS=30000` identically in both arms. Normal trace
  off, no pre-touch, no allocator instrumentation or strong-incumbent experiment.
- Generator-declared metadata is equal between Paro arms but asymmetric to
  DuckDB. Cross-engine observations do not qualify for parity certification.

## Schedule and analysis

Four batches: Probe-1, Control-1, Control-2, Probe-2. Query order is 04/11/74
in the first pair, 74/11/04 in the second pair. Use seeds 2026092411 and
2026092412 respectively. Each query/batch has three fresh normal processes,
one warmup (the cold invocation), three ABBA warm rounds per process, and one
separate diagnostic process. Thus each Paro arm/query has six cold blocks and
36 warm samples, with warm samples nested within processes. Use 10,000 draws
in the maintained cross-engine block bootstrap.

Compare Paro arms directly: compiler and C1 cold samples, per-process warm
summaries and overall medians. Report each batch-pair probe/control median
ratio as well as the combined ratio; never divide separate Paro/DuckDB ratios
to manufacture a speedup. A directional improvement requires both batch pairs
to agree; >10% compiler or warm regression triggers investigation, not deletion
or an automatic revert. These samples do not certify tails or non-inferiority.
Do not add samples or change thresholds after seeing results.

Preflight found ambient VM/application CPU activity. Preserve all observations
and record bounded process snapshots before/after batches. Do not run another
owned build/test concurrently with sampling. This cohort is explicitly not a
quiet-host certification; drift or new competing compilation makes attribution
inconclusive. Do not terminate unrelated processes or remove their slow samples.

Full typed results, multiplicities and required ordering must pass outside every
timer. Require cold target cache miss, normal compile_work, matching resources,
and valid compile/admission/execution receipts. Validate the first completed
cell before continuing. Stop on correctness/identity/capacity failure and keep
the partial campaign. No blessing, budget changes or retries-until-green.

Retain maintained RunOutput cells/receipts and one capture per diagnostic cell,
plus this registration and a conclusion. Total uncompressed archive <=16MiB,
each query/batch <=1MiB; no binaries, server logs or raw trace floods in Git.
