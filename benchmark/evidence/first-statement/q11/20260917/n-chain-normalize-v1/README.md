# Q11 N-CHAIN: pre-Memo CTE domain normalization

Date: 2026-09-17

## Result

This experiment does not justify enabling the chain in production.

The `chain` arm (N2) can construct the producer-side necessary date restriction and
preserve the Q11 result, but it changes the selected physical plan: at grant 2 the
runtime-filter contract drops from 2 to 1 and the admitted fingerprints change. Its
normal compiler/C1 pilot is not better than the clean off control. The `chain-only`
arm (N1) cannot satisfy the quality policy after the two exploration rules are
disabled; JoinRegion and SharedAggregate evidence remains missing and the search
expands substantially.

The experiment therefore establishes a negative/diagnostic result, not a production
optimization. The status remains `QualityPolicySatisfied + SearchIncomplete` for
the successful off/N2 searches; none of these runs is `ProofComplete`.

## Scope and reproducibility

- Clean source used for the experiment: isolated worktree commit
  `d4557653255a8c3ae223c54831e9b41865f88ece`
  (`feat(optimizer): diagnose pre-memo CTE domain chain`).
- Release binary: `/private/tmp/paro-nchain-target-clean/release/parod`
- Binary SHA-256:
  `12ca68e18786c7d5e44bfda0b1a08659b92c8242489ad9a41fa0262fb0a3ca0d`
- SQL: `benchmark/evidence/first-statement/q11/20260917/cross-group-domain-continuation-v1/11.sql`
- Data: TPC-DS SF1 Paro rowsets and the paired SF1 DuckDB database.
- Resource envelope: 4 workers, 2 GiB, the existing Q11 handoff policy and search
  budget.
- All diagnostic runs used a fresh server/data clone and returned the same 90-row
  typed and ordered digest:
  `9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`.

Diagnostic mode was read once at process start:

```text
off:        PARO_DIAGNOSTIC_NORMALIZE_CTE_DOMAIN unset
N2/chain:   PARO_DIAGNOSTIC_NORMALIZE_CTE_DOMAIN=chain
N1/chain-only: PARO_DIAGNOSTIC_NORMALIZE_CTE_DOMAIN=chain-only
```

`PARO_QUALITY_POLICY_HANDOFF=1` and `PARO_COMPILE_WORK_EVIDENCE=1` were held
constant. Diagnostic runs used statement tracing only for the existing bounded
diagnostic record; their wall-clock values are not normal C1 measurements. Normal
pilot runs used trace-off, fresh processes and cache-miss conditions.

The repository worktree was intentionally not used as the clean source: it contains
pre-existing staged, unstaged and untracked user changes. No user change was reset,
cleaned, or included in the isolated commit.

The quoted historical starting tuple from the task (`2246` syntheses, `1312`
published winners, `227` groups) was not reproduced by the isolated clean OFF run
(`2262`, `1311`, `215`). The normal pilot has four OFF blocks, but the strict
historical OFF reproducibility gate is therefore not passed. All treatment claims
below are within the same isolated binary and data/envelope, not a causal comparison
against that historical dirty/partially mixed tuple.

## Diagnostic control and treatments

| arm | optimizer wall (trace-on, us) | search stop (us) | quality satisfied (us) | groups / logical / physical | syntheses | recompute | published winners | quality evaluations | status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| off | 124,167 | 73,699 | 69,528 | 215 / 331 / 542 | 2,262 | 171 | 1,311 | 95 | satisfied + incomplete |
| N2 `chain` | 124,004 | 79,271 | 72,294 | 111 / 152 / 268 | 1,652 | 284 | 1,053 | 233 | satisfied + incomplete |
| N1 `chain-only` | 1,302,085 | 1,221,215 | — | 865 / 1,117 / 1,568 | 73,360 | 27,247 | 17,060 | 12 | not satisfied; incomplete |

The rule counters show that N2 did not eliminate the whole domain chain: it still
published 14 `PredicateTransfer` results. N1 pushed the date restriction into the
branches but, after disabling `CTE_FILTER_PUSHDOWN_RULE` and
`PREDICATE_TRANSFER_RULE`, it could not produce the remaining JoinRegion and
SharedAggregate quality packages.

N2 direct binding dispatch was 2 with 6 work units; N1 had 0/0. This is not evidence
that N1 found a better early path: it stopped without satisfying the required quality
bundles. The OFF control had 4 direct dispatches and 28 work units.

## Shape gate and plan comparison

The N2 deterministic shape checks passed:

1. the producer filter was built from `derive_producer_predicates`;
2. both UNION branches had the necessary date restriction at their `date_dim` input;
3. all four consumer filters remained above their CTE references;
4. aggregate-result predicates remained above the aggregate;
5. the 90-row typed/order digest matched OFF.

N1 also showed the structural date/consumer/residual properties, but failed the
quality-policy gate, so it is not a valid handoff plan.

The successful N2 plan was not physically equivalent to OFF. At grant 2:

| arm | aggregates | runtime-filter contracts | row-fetches | fingerprint (lower/upper) |
|---|---:|---:|---:|---|
| OFF | 4 | 2 | 0 | `11484202015574815693 / 15964561393389149331` |
| N2 | 4 | 1 | 0 | `5579745356907252357 / 9772924118627213876` |
| N1 | 3 | 0 | 0 | no quality handoff |

N2 also changed the physical fingerprints for the other grants and produced larger
estimated CTE/branch cardinalities. The same result digest is therefore not a plan
quality or execution-quality equivalence proof. OFF grant-2 had 6 gets, 9 filters,
4 aggregates and 7 joins; N2 had the same operator counts except for the reduced RF
contract. N1's grant-2 winner had 5 gets, 8 filters, 3 aggregates and 6 joins.

## Normal fresh-process pilot

These are four-block directional pilots per arm, not the registered 36-block
campaign and not a parity/non-inferiority claim. All valid samples are retained.

| block | OFF Paro C1 / W | N2 Paro C1 / W | N1 Paro C1 / W | DuckDB C1 / W |
|---:|---:|---:|---:|---:|
| 1 | 283.788 / 75.634 | 188.942 / 81.705 | 1407.009 / 84.876 | 141.524 / 109.601 |
| 2 | 191.216 / 78.074 | 249.186 / 83.447 | 1427.061 / 127.072 | 116.699 / 110.849 |
| 3 | 184.853 / 74.247 | 217.207 / 85.854 | 1487.731 / 115.512 | 116.823 / 114.683 |
| 4 | 190.173 / 74.775 | 201.048 / 73.882 | 1566.534 / 108.888 | 115.727 / 110.217 |

Pilot medians are approximately:

| arm | C1 median (ms) | W median (ms) |
|---|---:|---:|
| OFF | 190.695 | 75.205 |
| N2 | 209.128 | 82.576 |
| N1 | 1457.396 | 112.200 |

The N2 pilot has no repeatable C1 improvement and its W is not shown to be
non-inferior. N1 is a clear regression caused by the missing exploration work, not a
candidate production mode.

One additional compile-evidence sample per arm was collected for orientation only:

| arm | C1 (ms) | compiler (us) | optimizer (us) | rule (us) | syntheses |
|---|---:|---:|---:|---:|---:|
| OFF | 309.748 | 115,567 | 106,141 | 35,793 | 2,262 |
| N2 | 240.039 | 82,715 | 81,095 | 16,130 | 1,652 |
| N1 | 1,851.300 | 1,691,314 | 1,687,670 | 495,423 | 73,360 |

These single samples are not used as confidence intervals or as a cross-cohort phase
subtraction. The normal pilot does not certify cache miss through the auxiliary
occurrence helper: that helper expected occurrence 1 while the first target statement
was recorded as occurrence 0. Fresh-process/cache-miss harness conditions were used,
but the side-channel cache check is recorded as not captured rather than claimed.

## Implementation and tests

The isolated change adds only the diagnostic path:

- pre-Memo producer-domain normalization after single-reference CTE inlining;
- reuse of `derive_producer_predicates` and the existing `domain_transfer` contract;
- exact normalization-source proofs attached to the initial MaterializedCTE logical
  expression;
- diagnostic-only quality evidence access to those exact Normalization proofs;
- existing `SearchBudget::disable_transformation` for N1;
- minimal stale fixture updates required to compile the current production API.

The default/off path keeps the old evidence behavior and search rules. No default
stop policy, budget, cost model, frontier policy, quality threshold, or handoff policy
was changed.

Clean validation:

- `cargo check --locked --bin parod`: passed;
- clean release `cargo build --release --locked --bin parod`: passed;
- targeted predicate-domain, domain-transfer, quality-evidence, CTE facts,
  pushdown, and CTE transformation tests: passed;
- full optimizer library: 1,320 passed, 15 failures in existing/mixed areas
  (engine bound pruning, runtime/physical selection, singleton/dimension sharing,
  native deferral cache assertions, and related known fixtures). The failures were
  not blessed; full log: `/private/tmp/paro-nchain-clean-optimizer-tests.log`;
- SQL regress: not run;
- 36-block formal campaign: not run.

## Decision

Do not proceed to T2 or enable either mode by default.

The next focused investigation is the physical-quality consequence of the N2
producer restriction: why the normalization-only producer fact causes RF/source
response and branch cardinality choices to differ from OFF, and whether that missing
evidence belongs in the exploratory layer rather than in a pre-Memo necessary-domain
rewrite. N1 additionally confirms that JoinRegion and SharedAggregate remain
exploration-dependent. This is a new decision point; no claim of compiler ≤30 ms,
compiler <10 ms, parity, or ProofComplete is made.
