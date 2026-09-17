# E-LANE: obligation-only quality-lane experiment

Date: 2026-09-17

This is a diagnostic structure experiment. It is not a production policy change,
not a complete-search result, and not a parity campaign. The opt-in switch is
`PARO_DIAGNOSTIC_OBLIGATION_ONLY=1`; it is read once per process and is off by
default.

## Question

The experiment tests whether the optional quality handoff can stop after its
actual forced bindings and matching producer obligations, instead of falling
through to the ordinary global transformation agenda. Deferred ordinary tasks
remain in the agenda and are counted; they are not discarded. The public status
remains `SearchIncomplete` unless the existing quality handoff is independently
satisfied. No budget, rule, cost, frontier, grant, or quality-policy constant was
changed.

The implementation is committed as `e577daf1db72cd37fa6d0806170abf981892be1f`
(`feat(optimizer): isolate obligation-only quality lane`). The clean release
binary SHA-256 is
`104be7d20bb0e9f75fc611700299509b0bba315c2b21d5624e4c48fb6aeee155`.

## Clean diagnostic comparison

The three runs used the same committed source, binary, SQL, SF1 seed, 4 planning/
execution threads, 2 GiB limit, quality handoff setting, and diagnostic harness.
The two off runs bracketed the on run in fresh processes. The raw reports are
compressed in this directory. Raw report SHA-256 values before compression are
also listed so the archived evidence can be checked independently.

| report | switch | optimizer wall | syntheses | winners | groups | quality evaluations | plan SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `diagnostic-off-0.json.gz` | off | 78.669 ms | 2,246 | 1,312 | 227 | 96 | `ebe52e2d...` |
| `diagnostic-on-1.json.gz` | on | 11.584 ms | 124 | 124 | 31 | 1 | `5b138501...` |
| `diagnostic-off-2.json.gz` | off | 79.521 ms | 2,246 | 1,312 | 227 | 96 | `ebe52e2d...` |

The complete plan hashes and all counters are in the raw JSON. The off runs are
identical in fingerprint and search counters, so the switch is isolated. The on
run deferred 80 ordinary tasks, observed 20 non-root physical groups, and stopped
with `ObligationLaneExhausted`. It did not satisfy the quality policy: 3 bundles
and 4 facts were still missing, no producer dispatch had repaired them, and the
returned plan was the mandatory/P_safe fallback rather than the quality plan.

This is result C from the pre-registered decision rule: the lane is much cheaper,
but it omits a decision needed for the high-quality plan. It is not evidence that
the ordinary search can be removed, and it does not justify T2 phase integration.

Raw report SHA-256 values:

```text
04a3005ce03b5471b3a55d49e2ccdd1a6885612494fd376c04815c7b62494a96  diagnostic-off-0.json
5296a1d1be2c9678adbc2dde9ed3eb5ecc191724212572a6e2e263dcdb4a2f06  diagnostic-on-1.json
ccf128ea5c93d454e2d34b3aad37c5c7a07952c1af5a900471c5846a2a74865e  diagnostic-off-2.json
```

## Normal pilot

The normal pilot used the same clean binary and a separate trace-off/cache-miss
cohort. Each arm had four fresh-process blocks, full 90-row typed/value/order
validation, and the same DuckDB comparison. These are directional pilots, not the
36-block formal gate. All valid samples were retained.

| arm | Paro C1 median | DuckDB C1 median | paired C1 ratio (95% bootstrap CI) | Paro warm median | warm ratio (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: |
| off | 198.086 ms | 70.219 ms | 2.770 [2.520, 3.070] | 77.581 ms | 1.396 [1.330, 1.454] |
| on | 578.852 ms | 79.209 ms | 7.016 [6.585, 7.323] | 412.227 ms | 5.773 [4.914, 6.459] |

The on arm passed result validation but selected a substantially worse plan; its
on diagnostic plan SHA-256 was `5b138501...`, not the off plan
`ebe52e2d...`. The normal pilot therefore confirms that obligation-only stopping
cannot be enabled as production behavior. DuckDB drifted between the small arms,
so no precise cross-arm C1 effect is claimed; the degradation is nevertheless
large and consistent with the diagnostic plan loss.

Normal report SHA-256 values:

```text
8234eed8bde54a3fd6ad74b5ddd4d2ffd0873771d57eb77742a637420e049603  normal-off.json
b33bb5329893ef6485011dd2687fdc08b79d60302068ea5fa1366db5d0d4ffea  normal-on.json
```

The harness used seeded random ABBA within each process block, 4 threads, 2 GiB,
private Paro input copies, verified plan-cache misses, trace-off normal samples,
and full result validation. The report records the SQL/data/harness hashes and
build attestation.

## Tests and status

On the shared worktree after applying only the E-LANE code (without staging or
committing unrelated mixed changes):

* `cargo check --locked -p paro-optimizer --lib` passed.
* `cargo test --locked -p paro-optimizer --lib cascades::engine::tests::quality_production --no-fail-fast` passed: 9 tests.
* `cargo test --locked -p paro-optimizer --lib native_domain --no-fail-fast` passed: 21 tests.
* `cargo test --locked -p paro-optimizer --lib settlement --no-fail-fast` passed: 33 tests.
* A full `cargo test --locked -p paro-optimizer --lib --no-fail-fast` run executed
  1,334 tests: 1,321 passed and 13 failed. The failures are the existing mixed-tree
  physical-selection, grant-envelope, singleton-group and bound-oracle assertions;
  they are outside this diagnostic switch and were not blessed or changed here.
* Clean isolated release `cargo build --release --locked --bin parod` passed.

Whole-tree formatting still reports pre-existing mixed-worktree formatting
differences; no unrelated files were reformatted. SQL regress was not run in this
experiment. The full optimizer failures remain separate and are not blessed.

The experiment leaves the default path unchanged. Q11 remains
`QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`; compiler <=30 ms,
the <10 ms target, and DuckDB parity are not achieved. The next work must identify
the missing producers for the three bundles/four facts exposed by the lane, rather
than enabling a broad stop or deleting ordinary search.
