# Aggregate representation and ownership: exploratory validation

EvidenceId: aggregate-contracts-v1-20260924

Base source: e37ae93f. The candidate commit is recorded by each maintained
collector's source/build attestation. This registration precedes collection.

Interventions:

- Compose compact retained keys with dependent-state output projection;
  restore both directly at the aggregate emit boundary, including spill.
- Estimate disjoint equalities on the same integral column as a union, not
  independent events. Estimated NDV does not become a predicate-removal proof.
- Consume owned merge fragments; an empty destination may adopt the first
  non-empty fragment with the same accounting target and update contract.
  Keep fragment order and preserve every allocation's owner/allocator.

Only the candidate is sampled. Historical reports are context, not a control.
No causal speedup, performance non-inferiority or parity certification follows
from this pilot. The host has other user workloads which are not stopped.
The aggregate placement policy still requires a separate contextual comparison
contract; these changes do not claim that work is complete.

## Collection

One arm, Q04/Q11/Q74, each two fresh process blocks, three measurement rounds
and one warmup per process; one separate diagnostic process per query. Use the
maintained `tpcds_compare.py`, random seed 240924, bootstrap samples 10000.
Normal samples use binary results, trace off, `PARO_COMPILE_WORK_EVIDENCE=1`,
quality search policy, optimizer_verify off, explicit optional deadline 30000ms,
four workers and 2GB. Target occurrence zero must be a cache miss. Keep all
samples and validate complete types, row multiplicities and required ordering.
There is no sample rejection or performance threshold; invalid results stop
collection. This is not an optimizer completeness claim.

Reuse the immutable relocatable seed `/private/tmp/paro-migration-relative.u1PLBV`
(SHA256 256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927),
SF1 DuckDB database `/Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb`
(SHA256 568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7),
CSV source `tpcds-sf1/csv`, and queries from the local DuckDB TPC-DS extension.
Use the existing generator-declared metadata track: it is non-qualifying for
engine parity. Record per-query SQL identities in collector manifests.
The declared comparison runtime is DuckDB 1.5.5 (`benchmark/requirements.txt`),
native extension SHA256
85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68.
Verify the actual runtime before collection; do not upgrade dependencies.

Use one Cargo target serially and owned collector processes/ports. Archive only
maintained RunOutput manifests/cells/captures and a short conclusion. Total
uncompressed evidence budget 16MiB (bounded 4MiB per query, 4MiB shared validation).
No server logs, duplicate raw traces, expected-output updates or baseline bless.
