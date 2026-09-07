# Cold planning and Q11: 2026-09-07 checkpoint

This is a partial performance delivery, not completion of the cold-planning
target. Q11 steady execution remains faster than DuckDB under the measured
configuration. Fresh-process planning remains orders of magnitude slower.

## Implemented contracts

- `c76cd594`: distinguish discovery invalidation from application invalidation.
  A newly discovered alternative does not require reapplying every unchanged
  binding. Application observations compare exact bindings inside fingerprint
  buckets, retain declined matches, and replace obsolete fact observations.
  Statistics observations include the resolved cardinality envelope, rather
  than only the local recipe that references another group.
- `121d64b9`: bind local operator patterns and keep irrelevant descendants as
  group holes. TopN includes the projection/filter/scan path required by its
  vector and full-text implementations. Join enumeration and aggregate region
  rewrites retain complete boundary evidence: schema-only holes cannot yet
  supply all their source-lineage, uniqueness, and control-region facts.
- A checked, nested group-hole transport replaces the separate root-only path.
  Restoration uses one post-order walk. Duplicate, missing, unregistered, and
  root transport nodes are rejected before publication.
- Associative join enumeration is reused for an identical graph problem in
  the same target group. Exact encoded atomic inputs, normalized predicates,
  and consumed facts distinguish problems; binary parenthesization does not.
  Entries participate in the staging transaction and roll back with it.
- Staging searches the matching operator-key range instead of scanning the
  complete expression reuse index. Optional group allocation includes the
  output schema and logical contract, matching the reuse admission contract.
  Freshly rebound projections therefore cannot share an allocation event while
  requiring different groups.

There are no Q11 identifiers, SQL fingerprints, budget reductions, or cost
constant changes in this delivery. The existing search budget can still yield
an incomplete optional closure; these changes do not claim otherwise.

## Validation

- `make static`: passed, including workspace/all-targets clippy.
- `cargo test --workspace --no-fail-fast -j 4`: 6,324 passed, 85 ignored,
  zero failed, including documentation tests.
- SQL regress against a fresh data directory: 183/183 passed.
- New tests cover collision-safe incremental applications, new alternatives,
  root/child statistics invalidation, inherited cardinality invalidation,
  opaque input boundaries under a small construction budget, associative
  graph identity, and independently rebound projection allocation.
- Two EXPLAIN baselines changed. CTE estimated rows changed from zero to one;
  filtered vector queries now select exact index-backed vector search instead
  of the former TopN/row-fetch shape. Query results did not change. The initial
  loss of full-text/vector TopK caused by an underspecified local pattern was
  fixed, not blessed.

## Q11 evidence

Measured source: clean `121d64b9ba7f6d00aa4e1aa188ee02655bfc0952`.

Binary SHA-256:
`14823eff36adf6c04d40fc77f872111b6f43b93cce2ea61aa0ed7b922b09cd0d`.

SF1, four threads, 2 GiB, symmetric `none` uniqueness metadata, and Paro
`optimizer_verify=true`. The execution comparator used five fresh-process
blocks, two warmups per process, three ABBA rounds per block, and 10,000
hierarchical bootstrap samples. All 30 measured samples per engine matched
the complete 90-row multiset, schema, and ordering contract.

| Measurement | Paro | DuckDB |
|---|---:|---:|
| Steady execute/fetch median | 101.575 ms | 105.618 ms |
| First complete statement median | 3,362.762 ms | 108.716 ms |
| First EXPLAIN median, separate three-process diagnostic | 2,803.818 ms | 4.486 ms |

Steady geometric-mean ratio: **0.964431**, hierarchical 95% CI
**[0.952739, 0.983643]**. All five block ratios were below one. The comparator
qualified this evidence as faster than DuckDB; it does not establish an
execution advantage for other data scales, memory grants, or DOP values.

The EXPLAIN diagnostic used a fresh Paro server and a fresh DuckDB process for
each of three observations, alternating which engine was measured first.
It excluded connection/setup time and did not execute Q11. This is
process-cold planning, not disk-cold I/O. It is a diagnostic, not a second
ABBA confidence-interval claim. First-statement latency must not be presented
as pure planning latency.

Local evidence artifacts (benchmark reports are intentionally git-ignored):

- `benchmark/report/tpcds-q11-cold-121d64b9-20260907.json`
- `benchmark/report/tpcds-q11-explain-cold-121d64b9-20260907.json`
- `/tmp/paro-cold-static-20260907.log`
- `/tmp/paro-cold-workspace-tests-20260907.log`
- `/tmp/paro-cold-regress-final-results-20260907.log`

Execution reproduction:

```sh
benchmark/.venv/bin/python benchmark/corpora/tpcds_compare.py \
  --server-data-dir /Users/linjunhong/workspace/tpcds-sf1/paro-data \
  --listen 127.0.0.1:6433 \
  --duckdb-database /Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb \
  --dataset-source-dir /Users/linjunhong/workspace/tpcds-sf1/csv \
  --query-dir /tmp/paro-q11-query-20260907 \
  --report benchmark/report/tpcds-q11-cold-recheck.json \
  --start 11 --end 11 --metadata-track none --threads 4 --memory-limit 2GB \
  --warmups-per-process 2 --process-blocks 5 \
  --measurement-rounds-per-process 3 --random-seed 11
```

## Remaining design work

The cold-planning target is **not met**. Reducing repeated work is useful, but
does not remove tree reconstruction and settlement for region/CTE rules.

The next prerequisite is a Memo-native boundary-facts contract for source
lineage, uniqueness, statistics dependencies, and control-region ownership.
Logical and physical consumers must declare the evidence they read. A smaller
pattern cannot silently remove evidence needed by implementation selection.
Only then can region/CTE transformations preserve group references throughout
rewriting, and stage only changed shells with incremental fact propagation.

Further acceptance must include actual dependency wake-up tests for inherited
and CTE evidence, construction/read-work accounting, default-budget Q11 plan
preservation, search TopK access-path coverage, and fresh-process EXPLAIN
measurements. A cached-plan hit or the Q11 execution speedup is not evidence
that the cold-planning contract has been completed.
