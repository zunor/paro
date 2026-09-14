# Memo contract, fact lowering, and necessary-domain fixed point

## Scope

This record covers the three optimizer architecture tasks requested for the
2026-09-14 clean implementation branch:

1. separate nested runtime-filter build-domain proof identity from evaluation
   occurrence identity;
2. classify Memo fact reads, updates, publication, and invalidation, and lower
   logical facts through the same transactional Memo insertion contract; and
3. make necessary-domain propagation idempotent within an explicitly scoped
   fact/context fixed point.

The implementation is based on clean `8dfab512` and is split into these
commits:

| commit | contract |
|---|---|
| `7f09a8dc` | semantic runtime-filter build-domain identity and separate evaluation identity |
| `3c062449` | typed Memo fact updates, read scopes, and category-specific invalidation |
| `91cb64b0` | transactional logical insertion with Memo-owned facts |
| `a9cff285` | scoped, rollback-safe necessary-domain fixed point |
| `e173f3a7` | preserve conflicting same-category read revisions during normalization |

No budget, cost constant, rule set, frontier policy, default stop policy,
quality gate, execution engine, or resource envelope was changed.

## Implementation and correctness evidence

### Runtime-filter identity

The build-domain proof key now contains the canonical Memo build group, key
column, join side, logical fact fingerprint, and local statistics snapshot.
It intentionally omits the physical implementation fingerprint, so physical
alternatives of one semantic build domain can share a proof. The evaluation
occurrence key separately includes logical expression, goal, and physical
fingerprint. This prevents nested same-shaped build domains from being
deduplicated while retaining valid sharing within one semantic domain.

The optimizer tests cover same-build/different-evaluation sharing, distinct
nested build domains, fact/statistics invalidation, NULL/key mapping, and
composition ordering. The old nested-RF expectation that relied on the
ambiguous operator-only domain key was removed; the new test asserts two
distinct semantic domains.

### Memo reads and updates

Production code no longer uses the broad `group_mut` escape hatch for fact
updates. `Memo::update_group_facts` journals the exact group before the
closure, restores values on error, reports logical-vs-statistics changes, and
invalidates only the corresponding fingerprints. Structural insertion,
physical-frontier publication, fact changes, and search-ledger accounting
remain separate operations.

`ReadScope` records whether a task consumed logical frontier, physical
frontier, logical facts, or statistics. Physical tasks observe the precise
child frontier they use; structure-only readers do not subscribe to
statistics. Read-set normalization now refuses to combine two observations
when the same category has different revisions, so an older stale dependency
cannot be hidden by a later cursor. Disjoint categories for one group can
still be combined. Existing TaskRegistry and publication checks consume the
same scoped read contract.

`LogicalInsertionContract` carries operator encoding, scalar/layout proof,
logical properties, and cardinality into `Memo::insert_logical_with_facts`.
The structural insert and fact merge share one transaction journal, so
rollback/retry cannot leave a structural expression with facts from another
attempt. CTE producer/read dependencies continue to use their concrete
producer facts and statistics rather than registry revision alone.

### Necessary-domain fixed point

`DomainFactContext` scopes a fixed-point visit by relation, evaluation
occurrence, semantic context, logical facts, statistics, and binding facts.
Domain expressions use the existing scalar normalization and a finite
syntax-directed fingerprint; the implementation does not infer unproved
implications or expand unbounded DNF. A visit is recorded only after an exact
native landing succeeds. Unsupported boundaries, failed rewrites, and missing
evidence therefore cannot become completion evidence. Checkpoint/rollback
removes visits published by a failed transformation.

The real native transfer path and its tests use the same scoped identity for
projection/aggregate/filter routes and child paths. Duplicate exact landings
are not rebuilt, while different path/context/fact versions remain distinct.
Non-selected alternatives and the ordinary Memo search remain present; the
fixed point is not a replacement optimizer and does not claim global search
completion.

## Verification

### Rust optimizer

`cargo test --locked -p paro-optimizer --lib` on `e173f3a7`:

* **1320 passed**;
* **2 failed**, both pre-existing grant-fixture failures reproduced before
  this task's final commit: `aggregate::dimension_sharing::tests::nary_sharing_plan_is_stable_across_default_budget_envelope` and
  `cascades::planner::tests::mark_join_to_semi_is_an_explicit_isolatable_transformation`.
  Both call the production path with an expected grant that is not declared;
  neither is a RF, fact-update, rollback, or domain-fixed-point failure. They
  remain unmodified and unblessed.

Relevant RF, Memo fact, read-scope, transactional rollback, CTE producer,
domain transfer, native-domain, and cross-query/resource oracle tests are
included in the 1320 passing tests. Release validation also passed:

```text
cargo check --locked -p paro-optimizer -p paro-compiler --release
```

The release check emitted existing unused/dead-code warnings only.

### Benchmark harness

Using the repository's Python 3.14 environment (the system Python lacked the
DuckDB extension):

```text
make -C benchmark test \
  PYTHON_SYS=/Users/linjunhong/workspace/paro/benchmark/.venv/bin/python3 \
  VENV_DIR=/nonexistent/paro-benchmark-venv
```

Result: **134 tests passed, 1 optional SciPy test skipped**. The harness's
informational performance-gate messages report missing local baselines; they
are not a result for this optimizer task.

### SQL regress

The first full run with the shell's default file-descriptor limit stopped at
`Too many open files` in the spill/statistics setup. The server was restarted
with `ulimit -n 16384` and the full suite was rerun, without writing expected
outputs:

```text
151 passed, 33 failed, 0 skipped, 0 new
```

The 33 failures are retained in `sql-regress-error.txt.gz`. They are existing
EXPLAIN/plan-text and full-text/vector rendering differences, setup-dependent
missing-table cascades, and a memory-setting mismatch from the 2GB server
envelope; they were not blessed. Ordinary CTE, aggregate, NULL/duplicate-key,
transaction, and related optimizer semantic cases passed. The two grant
fixtures above remain the only optimizer unit-test failures; no unrelated
regression cleanup was mixed into this task.

## Clean Q11 pilot

This is a pilot and not a formal M1/M2/parity campaign. The source and binary
were clean and committed at `e173f3a7`:

| identity | value |
|---|---|
| source working-tree SHA256 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| release `parod` SHA256 | `bcad1df8ec3fbb221834cb545934211c84e0e584e9ff50e3854a39360b225565` |
| Q11 SQL SHA256 | `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8` |
| Paro data snapshot SHA256 | `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5` |
| DuckDB database SHA256 | `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7` |
| DuckDB version | `1.4.4` |

The run used the original SF1 Q11, binary protocol, 4 execution threads,
planning DOP 1, 2GB, per-process private data copies, first target SELECT,
plan-cache miss, one warmup, trace-off normal cohort, and one separate traced
diagnostic block. All four Paro/DuckDB measured result samples had the full
90-row typed schema, values, multiset, and order validation.

### Normal result

There were two fresh normal process blocks, with all valid samples retained:

| metric | Paro | DuckDB | paired result |
|---|---:|---:|---:|
| C1 samples (ms) | 196.419, 194.602 | 107.351, 108.992 | median ratio `1.807448`, 95% CI `[1.785478, 1.829688]` |
| C1 median (ms) | `195.511` | `108.171` | not at parity |
| W samples (ms) | 91.983, 91.855, 92.155, 129.295 | 102.648, 103.702, 103.000, 103.958 | median ratio `0.969432`, 95% CI `[0.888018, 1.150727]` |
| W median (ms) | `92.069` | `103.351` | formal non-inferiority not established |

Compile side-channel values attached to the two cold occurrences were:

| compiler (ms) | optimizer (ms) | rules (ms) | synthesis |
|---:|---:|---:|---:|
| 65.224 | 64.534 | 25.937 | 2500 |
| 64.720 | 63.993 | 25.785 | 2500 |

The diagnostic trace client time was `528.818 ms` and is excluded from C1.
The handoff policy was enabled only for this historically comparable pilot;
the default stop policy was not changed or switched to handoff. Search status
is **QualityPolicySatisfied + SearchIncomplete**, not `ProofComplete`; the
model gates are registered but not admitted. The exploratory no-handoff run
remained a full-search/BudgetLimited diagnostic and is not pooled with this
normal result.

Raw artifacts in this directory:

| file | SHA256 |
|---|---|
| `11.sql` | `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8` |
| `q11-memo-contract-clean.json.gz` | `6fde83e28f7929c7d9663469aad73112f475cda4743fd4a93f0edf817a0a2cbc` |
| `q11-memo-contract-clean.diagnostic.parod.log.gz` | `43d88a115606f0921f1b314f24c881bc5aded96b87711fd6d46e5b8caedb4ddb` |
| `q11-memo-contract-clean.block000.parod.log.gz` | `563354582520fbef76a976d0857c4ac7e52f64f23806b0993c5af973e14f9dfe` |
| `q11-memo-contract-clean.block001.parod.log.gz` | `3efbc0445034d53d27ee0e07e9b85b12dc5e20a1e1c108a404f8c6f4273b9754` |
| `q11-memo-contract-clean.oracle.parod.log.gz` | `ba9800ff361347254c44187a2511bd72acbadcd45d234fc3222d50a0178a2fba` |
| `sql-regress-error.txt.gz` | `d023fe851d95f4f6d9aedc4dd33c5b056c084ff2d67a951fd676cd3da9d3eab5` |

The normal logs are empty by design because normal logging is trace-off; the
diagnostic log is separate and bounded by the diagnostic cohort contract.

## Acceptance status

The three code contracts and their focused tests are committed. The clean
pilot demonstrates correct full-result handoff and preserves the warm
execution advantage, but it does **not** demonstrate compiler <=30ms, C1
<=200ms, formal W non-inferiority, M1/M2/parity, or ProofComplete. The two
unresolved grant fixtures and 33 SQL-regress text/setup failures remain
explicit. No performance claim is made from test counts, search status, or
the diagnostic trace.
