# HNSW artifact rebuild required

The August 2026 HNSW storage redesign is intentionally incompatible with
previous artifacts. Recreate every HNSW/vector index from its
`CREATE VECTOR INDEX` definition after upgrading.

The rebuild is required because the durable contract now:

- stores only immutable graph-construction fields, separate from search policy;
- uses the fixed-width, self-contained HNSW artifact envelope version 15. The
  artifact owns its canonical dense-vector region as well as its graph,
  predicate covering layout, metric preprocessing and statistics, so one
  generation partition may cover several base segments without borrowing any
  segment-local vector storage. Distance remains owned solely by the build
  contract; there is no JSON in the open path. An authenticated hierarchy of
  4 KiB payload checksums, 4 KiB checksum pages, and a compact root directory
  protects lazy random access;
- uses the version-5 graph layout: every hot level-0 adjacency is one
  sentinel-terminated, contract-sized fixed-stride link record, while sparse
  upper levels remain delta-varint encoded. Observed degree can no longer
  change the artifact width. With the standard M0=32 contract the record is
  exactly two cache lines, removing both the random CSR-offset read and the
  cache-line drift of a separate degree word without decoding at open;
- uses search manifest v2 (`json-debug-v2` and `binary-v2`), where every
  artifact carries canonical, generation-owned multi-segment coverage and its
  deterministic local point-id mapping; old single-segment manifest images
  are rejected;
- requires HNSW provider-config version 15 and build-contract version 11, which
  select deterministic frozen-wave construction, keyed Feistel point ordering,
  barrier publication, canonical unordered point-pair scoring (so cosine
  topology cannot vary with inverse-norm operand order), exact predicate-local
  covering runs, and the full configured construction beam on every graph
  layer. Exact/graph costing is
  definition-pinned for sequential covering scores, indexed gathers, and
  unique graph scores per `ef`. The three coefficients form one atomic profile
  with either a built-in revision or a non-zero offline-calibration id; partial
  and unlabeled overrides are rejected. The graph coefficient is consumed
  directly: average level-0 degree is descriptive rather than an upper bound
  on the number of unique scores produced per beam slot. Built-in
  distance-cost revision 3 also charges the eager-admission retry when a
  predicate is not expected to populate the final unfiltered `ef` beam with
  Top-K headroom; all costing inputs therefore describe executable physical
  work rather than a cardinality cutoff. The former
  `plain_scan_threshold` and `filtered_plain_scan_threshold` options have been
  removed: fixed row cutoffs cannot remain valid across `ef`, graph degree,
  covering availability, and calibrated hardware costs. Every immutable
  artifact now compares its actual mixed covering/base exact work directly
  with graph work;
- carries the complete provider configuration in CREATE INDEX WAL records, so
  recovery restores the same physical contract instead of reconstructing
  defaults. WAL images written before this field was added are intentionally
  rejected;
- persists per-point cosine inverse norms inside the HNSW artifact;
- streams generation-owned sidecar envelopes directly into their aligned
  package range. Construction no longer concatenates all source vectors or a
  second complete serialized artifact in heap memory;
- stores inline HNSW pages without block compression so graph links, inverse
  norms, and predicate topology open directly over the immutable segment mmap
  instead of pinning one page-cache allocation per segment. Inline and sidecar
  HNSW readers validate only metadata at open and authenticate graph chunks
  immediately before first use, rather than faulting the complete artifact for
  a monolithic page/package checksum.

Loaded artifact checksum sweeps run as bounded, low-priority tasks on the
instance scheduler. They retain only weak generation references, authenticate
at most 1 MiB before yielding, and expose completion/failure/stale/deferred
counters. No process-global integrity thread or ungoverned artifact-retention
queue remains.

Frozen-wave boundaries are durable implementation details rather than SQL
tuning policy. `proposal_wave_size` and `warmup_point_count` are therefore no
longer accepted by `CREATE VECTOR INDEX`; changing either requires a new build
contract and an index rebuild.

Search indexes are opened lazily. An old artifact therefore does not prevent
its segment or base table from opening: ordinary SQL remains available, and
vector search degrades to the exact scan path. The lazy segment capability
retains a rebuild reason and search telemetry reports the degraded capability
when the stale inline artifact is first opened, so
`DROP INDEX` followed by `CREATE VECTOR INDEX` remains an available recovery
path on the upgraded binary. `CREATE VECTOR INDEX` now materializes every
visible segment through the governed maintenance path before the catalog entry
can become `READY`; an admitted build that makes no coverage progress fails
explicitly instead of publishing a permanently incomplete index.
Current-format checksum or structural corruption fails the operation that
observes it in the foreground. The rebuildable secondary artifact is then
quarantined; subsequent searches retain table availability and use exact base
vectors while integrity failure telemetry identifies the required rebuild.

Paro does not infer missing fields, translate the previous graph format, or
silently rebuild an index during a foreground query.

Search manifest v2 is shared by every search provider. FullText and Sparse
search definitions must also be recreated before starting the new binary;
their provider artifacts may be unchanged internally, but their old manifest
images do not encode generation-owned partition coverage.

FullText and Sparse definitions also gained strict provider-config version 1
in the same release. Recreate catalog definitions that still use unversioned
`{"config": ...}` or `{"physical_encoding": ...}` payloads; unknown and
missing fields are now rejected instead of defaulted by individual consumers.
