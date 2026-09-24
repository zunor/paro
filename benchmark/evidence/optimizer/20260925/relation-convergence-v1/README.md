# Relation convergence: implementation and negative-result ledger

The implementation/experiment slice is delivered. **Default promotion, the full
correctness corpus gate, G-Stats/G-Cost, exhaustive optimality and DuckDB parity
are not certified.** Quality remains the default. No expected result was changed
to hide a corpus mismatch; no search budget was increased.

## What changed

- `2afc3745a`: operator-local semantic key/finite-domain algebra, definition-ID
  CTE publication and child-owned Filter estimates. A complete compound key plus
  a disjoint branch discriminator is a key; customer_id alone is not proven so.
- `211d198eb`: ordinary bounded inner-join regions use the joint-region physical
  response owner. Predicate support, exact choices and output bindings survive
  reconstruction. A typed single_stage strategy provides a controlled ablation.
- `1932674ba`: collated strings cannot use byte-domain separation as a proof.
- `28b927661`: finite-domain Filter estimation separates proven tautologies and
  contradictions from uniform point priors; it does not fabricate frequencies.
- `2589f90e1`: open unconstrained syntactic CROSS inputs inside connected regions.
  The first breadth run exposed Q18 selecting a roughly 2.7e12-row Cartesian
  intermediate. A real SQL regression and region reconstruction test cover the
  correction. Genuine disconnected/evaluation-fenced regions remain explicit
  fallback domains, not proof-complete search.
- `83b2c7ed3`, `f96d5f76a`, `ea595ead9`: TPC-H read-policy routing, explicit
  successful attempt acceptance, optional profiles, and failed/uncollected
  sample coordinates. Resource/setup failures retain their original causes and
  do not become fictitious timings or accepted attempts.

The first three ordered implementation items in
[the convergence plan](../../../../../docs/optimizer/pipeline-convergence.md)
have been implemented and tested. Broad validation was run, and found blocking
issues. That is not the same as passing the remaining migration gates.

## Registered latency pilots

Read [registration.md](registration.md) before interpreting any number. All
three cohorts retain all seven original cell packages, including negative and
slow samples. Each cell has three fresh blocks, full typed/bag/ORDER validation,
normal trace-off bounded compiler receipts and a separate Detail capture.
Four threads, 2GB, generator-declared metadata, verifier off. DuckDB is 1.5.5.
Observed VM, metadata-indexing and other host work means these are exploratory
pilots, not powered non-inferiority or parity certification. Cohorts are not
pooled and cross-cohort differences are not isolated causal speedups.

Final connected-region cohort, milliseconds (medians):

| Query/policy | Compiler | C1 | Warm | DuckDB C1 |
| --- | ---: | ---: | ---: | ---: |
| Q04 pipeline joint | 11.179 | 288.424 | 149.856 | 139.529 |
| Q04 quality | 18.840 | 229.218 | 129.296 | 123.437 |
| Q04 pipeline single_stage | 10.378 | 290.245 | 221.732 | 163.604 |
| Q11 pipeline joint | 4.854 | 162.616 | 93.067 | 73.276 |
| Q11 quality | 13.080 | 145.629 | 83.861 | 69.913 |
| Q74 pipeline joint | 4.533 | 127.900 | 61.346 | 92.056 |
| Q74 quality | 12.454 | 120.677 | 61.077 | 93.939 |

This does **not** demonstrate Q04/Q11 execution parity with quality, or a C1
improvement. Q04's single-stage ablation is directionally worse than joint
aggregation; simply copying DuckDB's aggregate shape is not supported. The
ordinary response DP also adds compile work: no claim that it restored the
historical 7ms Q04 pipeline is made. Q04 Detail reports 99 ordinary-response
transitions and 51 aggregate transitions, without a Memo.

Source cohorts:

| Directory | Clean source | Interpretation |
| --- | --- | --- |
| `pilot-initial/` | `1932674ba` | Initial semantic-key/ordinary-DP intervention |
| `pilot-domain/` | `28b927661` | Additional finite-domain selectivity intervention |
| `pilot-connected/` | `83b2c7ed3` (optimizer `2589f90e1`) | Connected CROSS correction |

Final binary SHA256:
`218e1ee87229c7f5e62db8faab4b70988f75e2c4b7bda2d8972fee2f9b327e1d`.
Each original `inputs.json` retains full source/build, dataset, SQL, metadata,
worker, binary and harness identities/settings. Physical fingerprints locate
plans; they do not prove equivalence or global optimality. The Q18-only cell is
a correctness diagnostic collected alongside tests, not a qualified speedup.

## Statistics fixtures

`quality-fixtures/` preserves initial and post-domain reports for both policies.
The two independent CTE expectations are 4 rows (disjoint branch tags) and 16
rows (overlapping duplicate branches). Initially they estimated 0 and 1;
post-domain they estimate 4 and 16. In the whole small fixture set, pipeline's
maximum q-error is 4 (p95 2), quality's is 2 (p95 1.5). These are not a complete
Q04 physical-occurrence audit or a declaration of G-Stats admission.

`q04-q11-plans.txt` records the post-domain diagnostic plans. Residual estimates
are child-consistent and Q04 can apply a four-reference ratio predicate at that
subset. The wide customer build choice and aggregate group-count uncertainty
remain; neither a fabricated customer_id dependency nor a forced build side
was introduced to conceal them.

## TPC-DS breadth: failures are part of the result

This is a verifier-on migration screen, not the ordered full CORPORA gate or a
performance campaign. Both arms attempt Q01–99 through explicitly separate
range campaigns; Q66's output capacity remains uncovered.

| Policy | Published result passes | Result/binding/resource failures | Uncovered |
| --- | ---: | --- | --- |
| pipeline | 96 | Q39 exact multiset mismatch; Q58 ambiguous item_id | Q66 capacity |
| quality | 95 | Q39 exact multiset mismatch; Q58 ambiguous item_id; Q64 20s timeout | Q66 capacity |

- `breadth-interrupted/`: pre-CROSS-fix partial pipeline attempt. Deliberately
  interrupted after isolating Q18's Cartesian plan. Preserve its partial states;
  it is neither a completed campaign nor a before/after speed ratio.
- `breadth-connected/*-run`: source `83b2c7ed3`. Q66 exceeds the registered
  36,096-byte normal-cell lease. The original manifest says Incomplete /
  CapacityExceeded; the last campaign summary is stale, and the maintained
  summary validator **rejects** that mismatch. No JSON has been repaired by hand.
- `breadth-connected/*-67-99-run`: source `f96d5f76a`, unchanged optimizer
  binary; both range campaigns complete, all 33 results pass. This does not
  retroactively complete either original 99-query campaign.
- All retained published cell payloads and compile captures pass their typed
  validators. That statement excludes the rejected original campaign summaries
  and does not mean failed SQL results passed correctness.

Q18 now passes full result validation. Q51 is slow on both current paths.
Q72 is a major pipeline execution regression signal (roughly 9s versus 1s C1
in this screen). Its pipeline Detail has zero joint or ordinary-response DP
transitions: inspect the unsupported/fallback region and actual plan, rather
than blaming the newly added DP without evidence. Quality Q95 spends roughly
11s before its first result despite a much faster warm execution. All these
samples are retained; they are not clean causal latency certifications.

## TPC-H and oracle applicability

The first raw-dbgen attempts failed setup on a trailing delimiter, and the old
publisher masked that with a registered-sample mismatch. `tpch/pipeline` and
`tpch/quality` retain those incomplete packages. Format-only normalized inputs
and all hashes are recorded in `tpch/fixture-inputs.json`; raw input was kept.

`*-csv` retain the next attempts. `*-retained` use `ea595ead9` to validate the
corrected failure publisher with actual SQL. Pipeline executes 22 queries:
16 pass the checked-in exact oracle, six fail. Quality records 15 passes, the
same six oracle failures, plus Q09 Host memory exhaustion at the registered
2GiB server/2GB query envelope. Its missing timing is explicitly uncollected,
not zero, and its original SQLSTATE 53200 survives publication. Failed attempts
are not accepted. No memory increase or result tolerance was used to pass.

The independent DuckDB audit on identical converted input also fails exactly
Q01/Q02/Q10/Q13/Q15/Q20 against the checked-in oracle. Five actual ordered
digests match Paro's failing digests; Q01 exposes exact floating-point comparison.
This establishes an oracle-applicability problem, not permission to bless six
failures or a complete new numeric certificate. The first raw-Decimal audit
was unsuitable for scalar_equals; it is retained with the corrected normalized
v2 audit and must not be treated as the verdict. No TPC-H timing is a cold/C1
comparison. Profiles are off; receipts are bounded and collected separately.

## Verification and remaining work

- Latest optimizer release: full workspace Rust tests **7000 passed, 0 failed**
  (85 ignored); strict Clippy and release build passed.
- Full SQL regress after CROSS correction: **185 passed, 0 failed, 0 skipped,
  0 new**. No blanket bless. The earlier scoped pg_settings expectation update
  records the added strategy setting, not query-result changes.
- Benchmark tests after failure preservation: **215 passed**. They cover
  acceptance, setup failure, missing sample identity and timeout continuation.
- Task-touched Rust files pass targeted rustfmt; memory-runtime and fallible
  vector-copy guards pass. Whole-workspace fmt still reports 57 unchanged files;
  the header checker reports 169 issues in 57 files byte-identical to task base
  `edc572964`. Full static readiness is therefore not claimed. Those unrelated
  files were neither reformatted nor silently exempted.
- Campaign/cell/capture validators were run with the limits above; original
  capacity-summary mismatches are explicitly not passes.

Prioritize migration blockers before default promotion: (1) Q58 output-name
binding and Q39's build-specific numeric contract; (2) Q72 fallback-plan quality
and broader region capability, using exact chosen plans; (3) data-pinned TPC-H
oracle audit and resource behavior; (4) Q66 schema capacity and failure-terminal
publication; (5) remaining write/search/graph/low-resource coverage. Re-register
and run a clean paired non-inferiority campaign only after correctness closes.
Do not increase a global search budget, hard-code Q04 build direction or declare
the 47ms cold residual to be a measured execution component.

No server logs, generated tables or database images are archived here. Original
owned diagnostics remain under `/private/tmp/paro-relation-convergence.BeCX81`.
SHA256SUMS covers retained files without a self-reference. Recovery branches,
other worktrees and user data were not removed.
