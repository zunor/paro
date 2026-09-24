# Q04 execution internals and aggregate-placement pilot

EvidenceId: `q04-execution-internals-20260924-v1`.

This is an exploratory, correctness-checked pilot, not a parity or performance
gate. The host has ambient VM/database activity; no user's process will be
stopped. All valid slow samples and failed attempts are retained.

## Intervention and comparison

The implementation under test gives selected string/dictionary decode one row
domain, borrows immutable string pages, uses bounded NEON bit-unshuffle tiles on
ARM64, and builds compact aggregate keys from prepared batch views. It preserves
the storage format, SQL arithmetic/NULL semantics, memory and spill contracts.
This pilot does **not** independently attribute each implementation's speedup.

The aggregate-placement question uses one binary and two explicitly different
search domains. `two-stage-enabled` enables all public rules;
`two-stage-disabled` disables `aggregate_dimension_deferral` and
`aggregate_dimension_sharing`. Both use the **budgeted** search policy and the
same 30,000 ms optional deadline, so failure to satisfy the structural quality
bundle is not itself a stopping-policy difference. This is an ablation, not a
proposal to disable rules in production. Inspect actual selected plans: rule
settings alone do not prove that a single/two-stage contrast was obtained.

Collect four cohorts in T/S/S/T order, each with the collector's minimum of two
independent process blocks (eight blocks total, four per arm), with three
measured warm repetitions after one warmup. Each block has a fresh Paro and
DuckDB process. Diagnostics are separate: one bounded COMPILE capture per
cohort, never a normal timing sample. A supplementary
instrumented ANALYZE may explain row reduction and operator work but cannot
certify speed or be subtracted from normal timing. A quality-policy reference,
if collected, is a separate arm, not pooled with the ablation.

## Fixed identity and envelope

- Source: this registration's implementation commit and clean source identity
  recorded by the maintained collector; release `Cargo.lock`/toolchain unchanged.
- Harness: `corpora/tpcds_compare.py`, including the recorded public
  `disabled_optimizer_rules` setting; use its RunOutput/cell validator.
- SQL: DuckDB TPC-DS `04.sql`, SHA-256
  `97c5894b2651a60ffe18177af6072bc8afd0e50a8f0000699974da771ceeffd3`.
- Immutable Paro SF1 seed: `/private/tmp/paro-migration-relative.u1PLBV`, SHA-256
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
- DuckDB database: `tpcds-sf1/tpcds-sf1.duckdb`, SHA-256
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`.
- Declared/actual DuckDB 1.5.5; native SHA-256
  `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
- Four workers, 2 GB, generator-declared metadata, binary PgWire, verification
  off, no pre-touch, normal tracing off. `PARO_COMPILE_WORK_EVIDENCE=1` is the
  bounded receipt observer; diagnostic Detail is never normal C1.
- Per-report random seed is `20260924 + block_index`; capture actual grants,
  physical/selection identities and stop reason rather than assuming equality.
- At most 64 MiB of retained campaign output, no archived server logs/raw event
  streams. All original samples stay in their bounded RunOutput cells.

## Decisions

Validate full typed result bag and ORDER for every measured sample. Stop on a
result error, seed/source identity drift or capacity exhaustion; do not bless or
retry until green. Verify whether scan predicates, RFs, join order and grants
are otherwise comparable. A different plan outside the intended placement
makes attribution to aggregate stages Incomparable, not a failed SQL result.

Report per-block cold/compile/warm and actual aggregate input/output cardinality.
With four blocks per arm and ambient load, all speed conclusions are
NotCertified regardless of the observed median. A structural decomposition
witness proves a legal merge, **not** execution benefit. Do not delete a final
merge based on observed near-unique groups, and do not substitute runtime row
counts for schema uniqueness proofs.
