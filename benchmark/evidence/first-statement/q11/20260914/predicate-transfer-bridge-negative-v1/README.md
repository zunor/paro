# PredicateTransfer direct-only bridge: negative pilot

## Decision

The `PredicateTransfer` direct-only optimization is rejected.  It preserved the
90-row typed and ordered result, but removed a semantic peer/closure path that
the Memo search still needs.  On a clean worktree at `074174a2`, the Q11 search
expanded from the preceding clean pilot's approximately 1,689 syntheses and
167 groups to 8,063 syntheses and 887 groups.  The diagnostic run became
`BudgetLimited` with `QualityPolicySatisfied=0`; the normal C1 median rose to
811.344 ms.  The change was reverted by `7b81fb7f` and is not a production
optimization.

This is a two-block implementation-regression pilot, not a formal C1/W,
M1/M2/M3, or parity campaign.  It does not change the default handoff policy,
budget, cost model, frontier width, or completeness semantics.

## Protocol and identity

- source worktree: detached clean worktree at commit
  `074174a2aaf94229c4258632e80959909cff7df0`
- source dirty: `false`
- source working-tree SHA-256:
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- binary SHA-256:
  `67daaeccf528e2199ef6fcccffbeb58c78b04d2d29657a500fed8740f70f7a2e`
- query corpus SHA-256:
  `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`
- Paro data seed SHA-256:
  `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`
- DuckDB database SHA-256:
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- harness SHA-256: `tpcds_compare.py`
  `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`;
  `benchmark_evidence.py`
  `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`;
  `tpcds_result_contract.py`
  `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00`;
  `tpcds_setup.py`
  `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fdabf2d171`
- DuckDB: `1.4.4`
- resources: 4 service threads, planning DOP 1, execution DOP 4, 2 GB
  maximum memory, one target statement, private data copy per process
- normal: two fresh-process blocks, one warmup and one ABBA measurement round,
  binary result format, trace off, verified plan-cache miss
- diagnostic: one independent fresh-process block with statement trace; it is
  excluded from C1
- source report SHA-256 (uncompressed):
  `df24f2a303eb212464bec04c5b390e7807df0a6d8ba862143618848714ba86ca`
- source diagnostic log SHA-256 (uncompressed):
  `df4f4d96a0b9456f2421b4f0e88de6e54bedf39e8d464ed47a3a689782b1a622`

The compressed report and diagnostic log in this directory have SHA-256
`eea3b74505fde9ccfb6e450cdc2fd98b1be574c98aa9d18faf7ad9e844d3cb00` and
`b6c945ae7146c9deea8a33b616023853827ca6ddb7a028d0eb8c41cdc982033b`,
respectively.  Compression used `gzip -n -9`.

## Result

Every normal sample passed complete result validation: 90 rows, exact logical
schema/column names, typed values, ordering, and the same result digest
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78` as the
DuckDB oracle.  Correct output therefore does not rescue the search regression.

| metric | Paro | DuckDB |
|---|---:|---:|
| C1 samples (ms) | 811.132500, 811.554917 | 106.856875, 109.497833 |
| C1 median (ms) | 811.343709 | 108.177354 |
| C1 ratio | 7.500684 | — |
| C1 bootstrap 95% interval | [7.411607, 7.590831] | — |
| warm samples (ms) | 146.625000, 145.204875, 146.192833, 145.388083 | 103.942500, 105.844500, 104.108000, 104.576542 |
| warm median (ms) | 145.790458 | 104.342271 |
| warm ratio | 1.394171 | — |

The two C1 values are the cold statement measurements from the two fresh
process blocks.  The four-value arrays are the within-block warm samples.  All
valid slow samples are retained.

## Search evidence

The diagnostic target statement reported:

| counter | direct-only pilot |
|---|---:|
| Memo groups | 887 |
| logical expressions | 1,494 |
| physical expressions | 2,618 |
| transformation bindings | 5,848 |
| transformation apply attempts | 4,294 |
| inserted transformations | 607 |
| cost syntheses | 8,063 |
| published winners | 3,386 |
| physical implementation requests | 1,415 |
| physical subproblem requests / reuse | 12,841 / 17,345 |
| registry requests / reuse / unique evaluation | 17,218 / 10,780 / 6,428 |
| quality policy satisfied | 0 |
| quality-policy frontier candidates / certified | 76 / 0 |
| quality-ready elapsed | 53.717 ms |
| compiler return | 677.863 ms |
| optimizer interval | 675.521 ms |
| search stop | BudgetLimited |
| freeze / handoff extraction | 2.213 / 1.131 ms |

The preceding clean pilot at `6867384c` recorded approximately 167 groups,
299 apply attempts, 1,689 syntheses, and 835 published winners with
`QualityPolicySatisfied`; those figures are a diagnostic reference, not a
pooled performance sample.  The direct-only change therefore did not merely
make a local bridge cheaper: it changed the search closure and consumed the
budget before a certified handoff candidate existed.

## Interpretation and follow-up

The local native shell was not enough to replace the owned semantic peer.  The
failed assumption was that local side-local equivalence implied Memo-level
equivalence, including semantic lineage, proof obligations, and downstream
competition.  The next implementation must preserve that lineage or provide a
Memo-native equivalent with an independent closure oracle before removing any
owned bridge.  No further C1 claim is made from this pilot; formal 36-block
campaigns, full SQL regress, and the four known baseline failures were not run
or blessed here.
