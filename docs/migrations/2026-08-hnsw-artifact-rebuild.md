# HNSW artifact rebuild required

The August 2026 HNSW storage redesign is intentionally incompatible with
previous artifacts. Recreate every HNSW/vector index from its
`CREATE VECTOR INDEX` definition after upgrading.

The rebuild is required because the durable contract now:

- stores only immutable graph-construction fields, separate from search policy;
- uses the fixed-width, self-contained HNSW artifact envelope version 13. The
  artifact owns its canonical dense-vector region as well as its graph,
  predicate covering layout, metric preprocessing and statistics, so one
  generation partition may cover several base segments without borrowing any
  segment-local vector storage. Distance remains owned solely by the build
  contract; there is no JSON in the open path. An authenticated hierarchy of
  4 KiB payload checksums, 4 KiB checksum pages, and a compact root directory
  protects lazy random access;
- uses the version-4 graph layout: every hot level-0 adjacency is one
  sentinel-terminated fixed-stride link record, while sparse upper levels
  remain delta-varint encoded. With the standard M0=32 contract the record is
  exactly two cache lines, removing both the random CSR-offset read and the
  cache-line drift of a separate degree word without decoding at open;
- uses search manifest v2 (`json-debug-v2` and `binary-v2`), where every
  artifact carries canonical, generation-owned multi-segment coverage and its
  deterministic local point-id mapping; old single-segment manifest images
  are rejected;
- requires HNSW provider-config version 12 and build-contract version 10, which
  select deterministic frozen-wave construction, keyed Feistel point ordering,
  barrier publication, exact predicate-local covering runs, and the full
  configured construction beam on every graph layer. Exact/graph costing is
  definition-pinned for sequential covering scores, indexed gathers, and
  unique graph scores per `ef`; runtime caps the graph coefficient by the
  immutable generation's observed average level-0 degree;
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
Current-format checksum or structural corruption is still a hard error when
that search capability is used.

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
