# Construction, selected DAG and physical observation ownership

EvidenceId: construction-dag-v1. Registered before implementation/collection.
Control source: df6627ea (clean re-op). Probe source/build will be recorded by
the maintained collector after the implementation is committed.

## Hypothesis and scope

Reuse canonical scalar construction across relational substitutions; retain
exact selected DAG structure independently of revision-sensitive properties;
carry a validated physical input observation into TaskRegistry without checking
the same immutable Memo twice. No search budget, quality policy, cost objective,
grant, verifier, execution or timer-boundary changes are interventions.

Independent oracles must cover changed facts, redirects, exact child choices,
evaluation fences and rule context. A reuse hit is not a completion proof.

## Collection and decision

Use the maintained tpcds_compare collector and bounded RunOutput. Q04/Q11/Q74,
quality policy, optimizer_verify off, four threads, 2GB, binary results,
generator-declared metadata. PARO_COMPILE_WORK_EVIDENCE=1 is the only normal
compile observer; normal traces are off. Separate one Detail capture per cell.
Two rounds of control/probe, each with three independent fresh normal process
blocks per query (six samples/arm/query), one measured cold and one measured
warm occurrence, 10,000 block bootstrap replicates. No concurrent workloads.
Round seeds are 2026092305 and 2026092306; visit Q04/Q11/Q74 in each C/P/C/P
batch. Control may use 9536a4df (this registration only) with the same code as
df6627ea. Use clean detached control/re-op probe sequentially in this checkout.
Retain every valid slow sample and failure. This is a directional pilot, not
formal parity/non-inferiority certification. A >10% warm or compiler median
regression requires investigation; no sample is removed because it is slow.
Report compiler, C1 and warm independently. Compare complete typed results,
admitted plans/choices/resources, search work and completion state; changed
plans/search work prevent an equal-work attribution, not result retention.

Reuse the immutable input/runtime declarations of
[property-ownership-registration.md](property-ownership-registration.md):
DuckDB 1.5.5 and its declared native/extension identities, SF1 SQL/data/seed,
toolchain/profile. Verify actual identities again before collection. Use one
Cargo target sequentially; no additional worktree or dataset is needed.
Bound total retained campaign evidence to 16 MiB, without raw trace/log archives.
Missing compile receipts stop compile claims rather than becoming zero time.

Validation: focused independent oracles, optimizer/workspace tests, check,
strict Clippy, benchmark harness tests, and compare-only high-FD SQL regress.
No expected-result updates, baseline blessing or history cleanup.
