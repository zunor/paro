# Native-domain rewrite journal pilot

This is a post-implementation pilot for commit `6867384cc48fb3ef2ff13aa26b57da3dc3e26fea`.
It is not a formal C1 campaign, parity result, or proof-complete search result.
The change replaces full `NativeShell`/layout snapshots used for speculative
domain-routing rollback with a local operator journal plus vector-length
checkpoints. It does not alter the search policy, budget, cost model, frontier
semantics, or production handoff policy.

## Protocol

- source: committed clean worktree at `6867384c`
- binary SHA-256: `7fe675c153ec6b0ce080febdd615de2663441ec654ffb269fa3f033c491fdb5d`
- SQL corpus SHA-256: `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`
- Paro seed SHA-256: `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`
- DuckDB file SHA-256: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- resources: 4 execution threads, 2 GB memory, planning DOP 1
- normal: fresh process, private seed copy, cache miss, binary result format,
  trace off, complete typed/ordered 90-row validation
- diagnostic: one separate traced process; excluded from C1
- normal blocks: 2; warmup 1; one measurement round per process

Harness file hashes are retained in the raw JSON. The harness command used
`PARO_QUALITY_POLICY_HANDOFF=1`, `PARO_COMPILE_WORK_EVIDENCE=1`, and
`PARO_COLD_WORK_EVIDENCE=0`.

## Results

| metric | Paro | DuckDB |
| --- | ---: | ---: |
| cold C1 median (ms) | 189.601 | 106.886 |
| cold C1 samples (ms) | 190.244, 188.959 | 107.211, 106.561 |
| warm median (ms) | 94.249 | 104.985 |
| paired C1 ratio | 1.7739 | — |
| paired W ratio | 0.8983 | — |

Both normal blocks passed cache-miss and trace-off checks. The diagnostic
statement reached `quality_policy_satisfied_us=52882` and
`optimizer=68822us`; it remained `QualityPolicySatisfied + SearchIncomplete`,
not `ProofComplete`.

The same diagnostic recorded 299 transformation apply attempts, 100 inserted
expressions, 1689 child-combination syntheses, 835 published winners, 343
implementation requests, 1042 subproblem requests, and 1393 subproblem
reuses. Admission selected class 2 with fingerprint
`5c29cf646706c8c8ba84000150211a6b`. These counts and the fingerprint are
reported as invariants for this pilot, not as proof that all optimizer work
was eliminated.

The preceding two-block pilot was from a dirty worktree and a different random
seed, so the small C1/optimizer difference is not attributed causally to the
journal. No formal M1/M2/M3 claim is made. The default production path was not
changed or claimed as improved.

## Artifacts

- `pilot-v1.json.gz`: full harness report
- `pilot-v1.q11.diagnostic000.parod.log.gz`: diagnostic server log

SHA-256:

```text
454534ed54ffdc77f8a06b5e78943823a07f536ca0c4e13b4aca92c0347925ea  pilot-v1.json.gz
42a5c1ca258259b4f07a43a0e662a39251f2165c52266f9234e2e850326690ca  pilot-v1.q11.diagnostic000.parod.log.gz
```

Known SQL/optimizer regression failures from the prior baseline remain
unchanged and were not blessed by this pilot; the full SQL regress campaign was
not run here.
