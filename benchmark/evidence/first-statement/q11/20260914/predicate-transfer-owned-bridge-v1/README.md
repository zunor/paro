# PredicateTransfer owned-IR bridge reduction

This is a bounded post-partition implementation pilot for commit
`4fd53088f034a4613d74eb344a3cfa54537026fb`. The 20260914 exclusive optimizer
partition identified B3 (rule apply/settlement/rollback) as the largest
actionable bucket. This slice removes the owned-plan instantiation only when a
native side-local predicate shell proves it is a complete replacement for the
legacy `FilterPushdown` result for the exact binding. Partial native shells
retain their owned semantic peer. No rule, budget, cost model, frontier,
stop-policy, or handoff-policy change is included.

## Contract

The completion gate is fail-closed. It requires one bound, type-matched,
depth-zero column-to-constant comparison that remains one expression after
the existing predicate normalizer. Conjunctions, disjunctions, residuals,
fences, and unsupported layouts remain on the owned path. A production Memo
binding test verifies that a complete native transfer stages one result without
instantiating an owned binding; a gate test rejects multi-expression and OR
predicates. Existing native-domain, staging, engine, and source-sensitive RF
oracles remain unchanged.

## Clean control/probe protocol

The control is parent commit `5ec3421e755d4659bec68dace7786b7a91b885ee` and
the probe is `4fd53088f034a4613d74eb344a3cfa54537026fb`. Each was built in a
clean detached worktree and run serially with the same harness, original 2418
byte Q11, seed, SQL, 4 execution threads, planning DOP 1, and 2 GiB limit.
Each arm used two fresh process blocks, one warmup, one ABBA measurement round,
binary results, private per-process seed copies, cache-miss verification and
trace-off normal timing. One separate statement-trace diagnostic block was
collected per arm and excluded from C1. All normal samples passed complete
typed/order validation for 90 rows.

Shared identities:

```text
query corpus:  a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503
source data:   4c73cc53e1567cad44341276a09589c209919e1002a7a59fbbedbf9d8e7532f3
Paro seed:     72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1
DuckDB:        568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7
result digest: 9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78
order digest:  65c8b356de484b4922445b09fad720b278db22a3678b81295018c9370be0c3d7
harness:       tpcds_compare 828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244
               benchmark_evidence 58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70
               result_contract 6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00
               tpcds_setup 9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171
```

The admitted class-2 physical fingerprint was
`c24392ee2773c58eea6048992173517e` in both diagnostic runs. The final-winner
choice stream, excluding timing fields, was identical. The source working-tree
attestation was clean in both arms:
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Search invariants and diagnostic result

The exact same diagnostic workload produced the following counters in both
arms:

| counter | control | probe |
| --- | ---: | ---: |
| Memo groups / logical / physical | 176 / 270 / 455 | 176 / 270 / 455 |
| transformation bindings / apply / inserted | 262 / 218 / 94 | 262 / 218 / 94 |
| cost syntheses / recomputes | 2053 / 197 | 2053 / 197 |
| published winners | 1158 | 1158 |
| implementation requests | 346 | 346 |
| subproblem requests / reuse | 751 / 894 | 751 / 894 |
| registry requests / reuse / unique evaluation | 969 / 273 / 686 | 969 / 273 / 686 |

The probe diagnostic occurrence measured optimizer 67.626 ms, including rule
work 16.081 ms; the control occurrence measured 73.051 ms and 19.652 ms. The
normal control and probe compiler-work scalars were respectively
70.196/69.640 ms and 56.708/57.482 ms optimizer, with corresponding rule
scalars 19.652/20.055 ms and 16.081/16.085 ms. The counters and admitted
fingerprint are invariant, but these are not a fixed-work replay of every
microsecond and the two arms were run at different wall-clock times.

The production change therefore has direct evidence of preserving the search
prefix and selected plan while avoiding the owned bridge on the proven subset.
It does not establish that the whole 23.685 ms B3 bucket is removed.

## Normal C1/W pilot

| arm | Paro C1 samples (ms) | DuckDB C1 samples (ms) | Paro C1 median | Paro W median | W ratio |
| --- | --- | --- | ---: | ---: | ---: |
| control | 277.2185, 313.1794 | 130.4716, 142.8015 | 295.1990 | 141.2105 | 1.1055 |
| probe | 224.7143, 253.3900 | 110.9669, 142.5986 | 239.0521 | 127.8181 | 1.0867 |

The report-level C1 ratios are 2.158655 (control) and 1.896949 (probe); the
small-sample bootstrap intervals are `[2.124742, 2.193110]` and
`[1.776946, 2.025056]`. The control/probe C1 difference is not claimed as a
causal estimate because the paired DuckDB medians also moved from 136.6366 to
126.7827 ms and the normal cohort has only two blocks per arm. The result
digest, typed schema and ordering were identical. Warm is likewise a pilot;
the probe median is not a formal non-inferiority result.

Both diagnostic searches ended as `QualityPolicySatisfied + SearchIncomplete`,
not `ProofComplete`. No deadline or reduced budget was used, and no normal
handoff policy was changed. The first invalid pilot invocation (missing seed
directory) produced no sample and is excluded from this archive.

## Artifacts and hashes

The compressed reports and diagnostic logs are retained here:

```text
control-q11.json.gz                 bd523884d5df0518de58c39f953ab11b7d4791174132da05117888eaf49443fc
control-q11.diagnostic.parod.log.gz 94f014e45ad78e09eaa388bd35b41c3e97f03a3ff76a5975e50ab250d14bb2b2
probe-q11.json.gz                   337b29a7cdeb25c60f1b7f5cc35f9a507528a2a57d21acebb0275e5d288893a7
probe-q11.diagnostic.parod.log.gz   b7d01ba2975db02b08434a37f1b91e70a2eddf0fac66e91c8cc108d6fb048f17
```

The underlying uncompressed report hashes are control
`50f019c87fff7962bff1283370fa94b495053247a5f6b60baea3c80a39ca59fc` and
probe `0c5f8b0d8e26eaa504c6a9a5a7b7fa1fe6159c5babbb7adba03bec9cdfc99c02`.
The uncompressed diagnostic-log hashes are control
`3d186a837b468c9fd695bf87fbf1f77c76910078c585ec327f3b40fb1ccf60e2` and
probe `38ec75f2b068d8b26551cf4163f5aa1313b0d826470e94115ad9a8acdd5e8347`.

## Scope and follow-up

This pilot does not claim M1/M2/M3, default handoff parity, ProofComplete,
formal W non-inferiority, or a 30 ms compiler target. The five known optimizer
baseline failures, full SQL regress, and the separate T1 W campaign remain
unmodified and unblessed. The next B3 investment should target a separately
proven late construction/settlement path only after a fixed-work measurement;
the broad PredicateTransfer direct-only experiment remains rejected.
