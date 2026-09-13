# B3 native-shell compaction pilot

This is a post-partition implementation pilot for commit `1e21fd862753a5cafadd59bcaeef40d50a4bc8f1`.
The partition in `../work-partition` identified B3 rule apply/rollback as the
largest actionable bucket: 23.685 ms of a 67.066 ms instrumented optimizer
interval, or about 60% of the 39.5 ms previously unaccounted share. The
implementation removes redundant native-shell compaction/layout walks in the
existing native rule paths. It does not change the rule set, search budget,
cost model, frontier comparison, stop policy, or handoff policy.

## Status and provenance

This is an implementation pilot, not a formal M1/M2/M3 campaign. The harness
report correctly records `source.dirty=true` because unrelated user changes
were present in the shared worktree. Those changes were not committed by this
task and remain untouched. Consequently the pilot is not used as clean-source
performance evidence or as a causal comparison with the earlier clean
partition cohort.

- source commit recorded by the harness: `1e21fd862753a5cafadd59bcaeef40d50a4bc8f1`
- binary: `/Users/linjunhong/workspace/paro/target/release/parod`
- binary SHA-256: `060f98d1d5cffe9cdd9f4fbb2976474c55994d504c9a2c44cd968b33385edb8d`
- SQL corpus SHA-256: `65c8b356de484b4922445b09fad720b278db22a3678b81295018c9370be0c3d7`
- Paro seed SHA-256: `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`
- DuckDB SHA-256: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- resources: 4 threads, 2 GiB memory, planning DOP 1, binary result format
- normal: 2 fresh process blocks, trace off, cache miss, one warmup, complete
  typed/order validation of 90 rows
- diagnostic: one separate traced process plus the additive B0--B12 ledger;
  excluded from normal C1
- harness hashes are retained in `pilot-v1.json.gz`

The diagnostic environment used `PARO_QUALITY_POLICY_HANDOFF=1`,
`PARO_COMPILE_WORK_EVIDENCE=1`, and `PARO_COLD_WORK_EVIDENCE=0`. The normal
sample was not run with statement trace.

## Results

| metric | Paro | DuckDB |
| --- | ---: | ---: |
| cold C1 samples (ms) | 188.627, 194.718 | 112.594, 108.798 |
| cold C1 median (ms) | 191.673 | 110.696 |
| cold C1 p95 in this 2-block pilot (ms) | 194.718 | 112.594 |
| warm samples (ms) | 95.765, 95.362, 96.470, 94.113 | 104.635, 106.324, 104.694, 104.144 |
| warm median (ms) | 95.563 | 104.665 |

The paired cold ratio is `1.731552` with the harness's small-sample 95% CI
`[1.675278, 1.789716]`; the warm ratio is `0.909263` with CI
`[0.900046, 0.920603]`. These intervals are not formal power or tail
certification. Both normal blocks passed cache-miss, trace-off, and complete
typed/order validation; the result digest matched DuckDB.

The two normal compile-work scalars were:

| block | compiler (ms) | optimizer (ms) | rule (ms) |
| --- | ---: | ---: | ---: |
| 0 | 54.741 | 53.852 | 20.752 |
| 1 | 57.791 | 56.810 | 21.792 |

The Q11 ledger rows for the diagnostic occurrence measured B3 at 20.704,
19.787, and 20.797 ms. The other representative buckets were B5
3.035--3.120 ms and B10 0.403--0.429 ms. The ledger remains additive; its
unclassified residual was 2.345--2.819 ms and was not assigned to another
bucket.

The diagnostic occurrence reported 1,689 cost syntheses, 835 published
winners, 299 apply attempts, 100 inserted expressions, 343 implementation
requests, 1,042 subproblem requests, and 1,393 subproblem reuses. These are
current-pilot counters, not a claim that they equal the clean partition
cohort: the report is dirty and its surrounding mixed changes/cohort differ.
The selected physical fingerprint was
`5c29cf646706c8c8ba84000150211a6b`; the 90-row result was valid. The search
state was `QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`.
The diagnostic actual search stop was 53.846 ms, quality policy was satisfied
at 52.746 ms, freeze was 2.068 ms, and compiler return was 70.014 ms.

## Interpretation

This pilot is consistent with a small B3 reduction, but it does not prove a
causal end-to-end improvement: the clean baseline and this run are different
cohorts, and the source was dirty. The code path is a policy-neutral structural
optimization: it reuses the existing post-order native shell when the
reachability proof already establishes that representation, and computes the
root output layout during the one required compaction pass. It preserves the
owned semantic peer where that is required for Memo lineage and proof
obligations.

The measured optimizer remains about 54--57 ms, above the intermediate
compiler target of 30 ms. The next safe target is the remaining repeated
PredicateTransfer native/owned construction and statistics refresh, but it
must be measured against a clean fixed-work replay before implementation. The
previous broad direct-only PredicateTransfer experiment is a registered
negative result and must not be repeated without a new semantic proof.

## Tests and omitted work

Passed for the implementation: `cargo check --locked -p paro-optimizer`,
native join preaggregation tests (3), native join subsumption tests (4), and
the staging tests (8). The full optimizer library run remains 1,234 passed and
5 known baseline failures; those failures were not blessed or changed. Full
SQL regress, formal 36-block W/C1 campaigns, default handoff activation, and
ProofComplete were not run in this pilot.

## Artifacts

- `pilot-v1.json.gz`: complete harness report
- `pilot-v1.ledger.jsonl.gz`: additive B0--B12 ledger
- `pilot-v1.q11.block000.parod.log.gz` and `pilot-v1.q11.block001.parod.log.gz`:
  normal server logs
- `pilot-v1.q11.diagnostic000.parod.log.gz`: diagnostic server log
- `pilot-v1.q11.oracle.parod.log.gz`: oracle server log

SHA-256:

```text
40f92dc4bf203bb59fb76225666ae67d2618f51bffd3d3338737bfba144962e3  pilot-v1.json.gz
cf248fb3dd549d2a168544a3a7cee1b58489b36a8cabb080c4808b592c29531f  pilot-v1.ledger.jsonl.gz
fbd8df511533f4aa49cfb0cfa354c18f50d4bd5337847b66f1223c9976229b12  pilot-v1.q11.block000.parod.log.gz
09a65c3564d248c9743255719ea3fee4089c7e5b6cd470f834eae0a88bb56a91  pilot-v1.q11.block001.parod.log.gz
d20bd718509752da18af98b06d2018bba7e35f7d2b695cc1a26ba00d885f5dbb  pilot-v1.q11.diagnostic000.parod.log.gz
38800222cdeab7c12348c168c38414924c546b23f1c773dabdd94e9ea539b10e  pilot-v1.q11.oracle.parod.log.gz
```
