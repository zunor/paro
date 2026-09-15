# Native Memo factory / resident publication v1

Date: 2026-09-15

This archive records the production-path vertical slice for the Memo-native
alternative construction task.  It is intentionally separate from the older
execution-layer and low-allocation experiments.  The implementation uses the
existing planner-session identity catalogs, `ResidentNodeContract`, staging
transactions, `GroupRef`/`ScalarExprId`, and existing fact/read contracts.  It
does not add a second optimizer, a cross-Memo seed, a new cache, a new
scheduler, or an execution-layer change.

## Scope and implementation

The common contract now carries operator fingerprint and encoding, scalar
roots, output columns/layout, and typed input fact witnesses.  Settlement and
native preparation share the session identity interning helper.  Native
`PredicateTransfer` and `AggregateDimensionDeferral` refresh statistics and
produce their resident contracts inside the same sidecar transaction that
stages the shell.  Staging validates the exact output identity and native
Memo/local input facts before publishing; cancellation, failure, and rollback
therefore cannot expose a half-prepared alternative.  Native refresh uses a
one-operator adapter for the existing statistics API; it never imports an
owned descendant tree into the Memo or a second arena.

The contract keeps structure, occurrence rebinding, and fact evidence
separate.  `Settlement` fact indices and native Memo/local witnesses are
different enum variants, so a settlement-local integer cannot be interpreted
as a Memo fact identity.  A resident contract is evidence for staging, not a
replacement for the current Memo read.

## Reproducible pilot

The result is a five fresh-process-block pilot, not a formal gate.  It used
the original SF1 Q11, 4 execution threads, 2 GiB, private per-process data
copies, normal trace-off/cache-miss measurements, and the existing quality
handoff policy.  Control and probe were run serially with seeded ABBA ordering;
the diagnostic block is excluded from C1.  The release binary was built from
the dirty worktree at commit `0e0f02665229b7607856476d007a25567a9ac1c1`.

| metric | Paro | DuckDB |
| --- | ---: | ---: |
| cold C1 median / p95 (ms) | 165.576709 / 177.522041 | 109.623500 / 110.111917 |
| warm median / p95 (ms) | 70.494854 / 72.673958 | 106.068375 / 111.310958 |
| cold ratio, hierarchical 95% CI | 1.530824 [1.498892, 1.579147] | — |
| warm ratio | 0.660408 | — |

All cold samples were retained in `s3-normal.json.gz`: Paro
`[177.522041,166.043291,165.576709,164.192250,164.333709]` ms and DuckDB
`[109.264375,108.223709,109.745375,109.623500,110.111917]` ms.  All ten warm
samples per engine are also retained there.  Every measured Q11 sample passed
the 90-row typed schema/value/multiset/order contract and verified plan-cache
miss; normal statement traces were absent.

The side-channel compile work was stable at 2,061 cost syntheses per block:
compiler `57.894–58.947 ms`, optimizer `57.157–58.152 ms`, and rules
`19.993–20.499 ms`.  The independent diagnostic block reported approximately
75.500 ms optimizer and 77.069 ms frozen compiler return; its trace and the
normal block logs are archived separately.  These figures do not establish
compiler <=30 ms, M1/M2, warm non-inferiority, or parity.

The search state in every arm is
`QualityPolicySatisfied + SearchIncomplete`; it is not `ProofComplete`.
The pilot therefore demonstrates a valid, executable contract path and
directional C1 evidence only.  It does not claim that the 5-block result is a
formal performance acceptance or that the observed plan/work-closure change
is entirely due to resident lowering.

## Work-closure and validation notes

The earlier bounded work partition found that the target path's remaining
cost is not proven to be a single settlement or statistics bucket.  The
current slice removes the duplicate identity/lowering path for the target
native alternatives, while preserving logical alternatives, exact child
choices, fact invalidation, CTE producer/consumer ordering, runtime-filter
proofs, budget, and final FrozenCandidate validation.  Native and settlement
focused tests cover contract consumption, identity reuse with fresh facts,
native shell staging, output-layout checks, and exact choice preservation.

Validation run from the current worktree:

```text
cargo check --locked -p paro-optimizer
cargo test --locked -p paro-optimizer --lib
make -C benchmark test
cargo build --locked --release -p paro-server --bin parod
```

The optimizer run completed 1322 tests and retained two known fixture
failures (`expected grant is not a declared class`); neither was changed or
blessed.  Benchmark tests passed 134/134.  SQL regress, run with
`ulimit -n 65536`, completed 151 passed / 33 failed / 0 skipped.  There was no
FD exhaustion; existing EXPLAIN/PROFILE, fulltext/vector, setup/grant and
memory-setting differences remain unblessed and are not attributed to this
slice.  The source was dirty during the pilot, so this archive is not a clean
formal acceptance artifact.

## Hashes

- report: `8d102cf1ff823f41b615ec00f291785936312848b87227df9ed4735ebe83834c`
- diagnostic log: `6ca105aac288d2b9348ba6f6eddd3dc0ce8a11aa3022adb6b268b4eb85b4a79c`
- release binary: `0d28f9f1353106f860eec5714a0f5589f4b72291fc8f38db54ee0de25624ed8a`
- Q11 SQL: `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`
- `tpcds_compare.py`: `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`
- `benchmark_evidence.py`: `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`
- `tpcds_result_contract.py`: `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00`
- `tpcds_setup.py`: `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171`
- Paro data: `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`
- DuckDB database: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`

The full raw JSON, diagnostic log, per-block logs, and input SQL are kept in
this directory.  No benchmark baseline was blessed.
