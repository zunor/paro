# Local necessary-domain publication: registered pilot

2026-09-13. Hypothesis: the selected native local closure is constructed but
rolled back because optional group allocation omits its input groups, whereas
group reuse compares them. Distinct restricted inputs must not share an
allocation identity. This is a publication-contract repair, not a priority,
budget, quality-policy, cost-model or executor change.

Before/after pilot: two fresh process blocks each, original archived Q11,
same immutable SF1 seed, 4 threads, 2 GB decimal memory limit, binary protocol,
quality handoff enabled in both arms. One warmup and one ABBA measurement round
per process; complete typed ordered results and cache-miss checks. Bootstrap
200, seed 1. Keep every valid sample. Normal trace-off; diagnostic separate.
Small sequential cohorts do not constitute formal M1/parity acceptance.

Pre-fix diagnostic isolates seven long native constructions followed by
allocation-identity errors, then successful ordinary one-hop publications.
The first source is logical 71 in this diagnostic only, not a production key.
Do not attribute all search time or all repeated dispatches to this defect.

Existing mixed user changes remain outside this task's commit scope.

Follow-up registration (after the initial A/B, before these runs): remove all
temporary diagnostic code; run final default control for two fresh blocks,
then final handoff for four fresh blocks, otherwise identical settings. Retain
the initial after pilot as well. These confirmation cohorts test repeatability
and same-binary execution quality, not a formal milestone campaign.

## Result: publication defect reproduced; patch NOT admitted

The candidate patch and its production regression test are preserved in
`candidate-not-admitted.patch`. They were removed from production source after
the default-path control exposed a severe regression. The release binary was
rebuilt from the restored worktree. No policy, rule budget, cost model or
executor change is shipped. Existing mixed changes were not reverted.

The allocation ledger hashed operator/context/schema/properties but omitted
ordered child groups; the preceding reuse lookup compared canonical child
groups. Distinct restricted inputs therefore collided. Fresh debug shows all
seven long native constructions rejected with
`optional Memo group allocation identity was reused without reusing its group`
(PredicateTransfer, rule 10026); ordinary bindings subsequently published.
The CTE multi-output test independently reports the same error for rule 10024.
It is not a one-output reservation exhaustion or a column-rebinding rejection.

Adding ordered canonical child groups to allocation identity makes the long
closure publish in the same Memo. The SQL domain derivation, residual placement,
quality policy and precise physical choices are unchanged. This does not turn
a quality-ready root into a complete-search proof.

## Normal SELECT results (all valid samples retained)

| Cohort | blocks | Paro C1 median / p95 ms | DuckDB C1 median ms | paired C1 ratio [95% CI] | Paro W median / p95 ms |
| --- | ---: | ---: | ---: | --- | ---: |
| before handoff | 2 | 262.571 / 262.637 | 109.970 | 2.387944 [2.350890, 2.425582] | 91.730 / 101.234 |
| candidate handoff initial | 2 | 234.720 / 234.722 | 108.458 | 2.164307 [2.138545, 2.190380] | 101.671 / 103.865 |
| candidate default final | 2 | 33504.388 / 33722.482 | 163.812 | 211.379380 [162.222579, 275.431711] | 148.628 / 149.581 |
| candidate handoff final | 4 | 236.713 / 288.282 | 109.016 | 2.269636 [2.127556, 2.487782] | 103.175 / 142.171 |

Final handoff C1 samples are `[236.378583, 233.983583, 237.046792, 288.282250]`.
Final handoff W ratio is 0.991573 [0.921565, 1.063653]; default W ratio is
1.319439 [1.222100, 1.389327]. The slow fourth handoff block is retained.
Handoff median improves by about 10%, but W is worse than the before pilot;
machine drift and changed selected plans prevent claiming invariant execution
performance. Final default and handoff share one binary. Neither M1 nor parity
passes. These small cohorts are not formal acceptance campaigns.

Every normal cohort passed complete 90-row typed schema, order and result digest
checks; C1 cache misses were verified outside the timer. Original Q11 and the
same immutable seed, 4 threads / 2 GB envelope and binary protocol were used.
DuckDB drift in the default cohort is visible and is not removed or normalized
away. Before default's historical 1388 ms is context, not a same-campaign
counterfactual default measurement.

Binary SHA-256:

- before: `b5252fcea45e73ad8cabeec4ac0720df9310a57e6d2d1e4ecce513d19f5bb8e1`
- initial candidate: `60d8b10382e28abfe8e70d23d7904f51455d17e2705483e1eed336ecfeee6b9a`
- final candidate without temporary diagnostics: `e30de3022debc39fa41ad77400634daab694f4370fa78b96ae45f0d329365399`

Full source/harness/SQL/data identities are in each report. They attest the
dirty worktree, not a clean-HEAD reproduction. Final candidate artifacts do
not describe the restored production source or its rebuilt binary.

## Same-SELECT diagnostic attribution

Before versus initial candidate, respectively:

- quality satisfied: 124.839 / 92.023 ms;
- compiler return (statement elapsed): 145.930 / 108.294 ms;
- direct binding dispatch/work units: 21/204 versus 7/36;
- cost synthesis: 4903 / 2966; required recomputes: 1012 / 356;
- quality evaluations: 227 / 250 (not reduced).

These are separate diagnostic cohorts, not normal C1 stage subtraction.
Within the initial candidate SELECT: first long binding source 71 publishes
logical 110 at 32.172 ms; producer 789 at 35.845 ms is consumed by executable
root 810 at 36.152 ms. The final producer retains this logical 110, not the
ordinary binding's logical 113. Its two date-side stacked filters are later
merged by task 195 (`95/94 -> 132`, 49.582 ms) and task 214
(`103/102 -> 133`, 49.745 ms). Final root 1775 qualifies at 88.526 ms;
policy satisfaction follows at 92.023 ms. These IDs are diagnostic identities,
never production dispatch keys.

The first proven roundtrip was failed publication, not a need to freeze a root
after every logical hop. Remaining physical event loss prevents attributing
49.745 -> 88.526 ms wholly to queueing or pricing. Root 810 already exists at
36 ms; its exact missing quality obligations were not independently isolated.
Coalescing domains at an existing legal leaf filter is a next local hypothesis,
not implemented or claimed successful in this experiment.

The default-path regression is real, not diagnostic client overhead: normal
C1 is 33.5 seconds. Its SELECT diagnostic records 380 groups, 7774 logical and
13280 physical expressions, 491390 cost syntheses, compiler return 32395.348 ms,
and **Deadline + SearchIncomplete**, not ProofComplete. AggregateNonNullInput
accounts for 19199.951 ms of combined binding/apply accounting with matched=0,
applicable=0, constructed=0 and published=0. PredicateTransfer records
2859.450 ms, 124744 matches and 7393 publications. This exposes expensive
search after removing erroneous allocation rejection; it is not evidence that
all these combinations are duplicates. A safe, bounded applicability/matching
path for AggregateNonNullInput and the newly exposed domain variants must be
resolved before this global allocation correction can enter production.

The concrete matching amplification is in `PatternScope::NonNullInput`:
Filter/Order/TopN/Limit recursively expand alternatives leading toward Get.
The active set is path-local, so shared downstream failures are revisited;
deduplication happens after traversal. Observing an already read group does
not charge another visit, and an empty downstream result does not reach
successful-combination charging. The repeated failure traversal therefore
also bypasses the work-admission checkpoint on those edges. In this diagnostic,
g94/e119, g96/e122, g98/e125 and g100/e128 report 24/25/26/27 reads but
run-to-dependencies-ready intervals of 1196650/2388871/4815274/9601673 us.
This is not proof of the exact number of Filter alternatives per layer;
it identifies a shared-subgraph failure traversal/charging defect, not a
reason to delete legal alternatives or reduce the configured budget.

## Execution and resource diagnostics

The initial candidate profile is a separate EXPLAIN ANALYZE, not C1. Date scans
both output 730 rows. Partial aggregate inputs remain 1096053/289524 rows;
final aggregate inputs are 76098/22804. Its store source scans 2880404 rows,
web source 289524; customer sources each 100000. Residual filters remain above
final aggregation. Profile RSS is 439451648 bytes; it is not a normal C1 peak.
No executor tuning was performed. Exact image identity with normal trace-off
samples and all RF differences have not been fully established.

## Tests, withdrawal and next decision

The proposed regression test uses Memo -> optimize_for_grants -> frozen
selected choices -> selected_quality_bindings -> native staging/publication.
Distinct restricted branches remain distinct; repeat requests add no groups;
rollback restores groups/payloads and allows retry; no owned staging arena grows.
Removing only child identity encoding reproduces the allocation error on the
second closure. Restoring it passes. CTE multi-output likewise changes fail ->
pass under the patch. Counterfactual error was observed directly, not blessed.

Under the candidate: domain_ 46, transformation module 63, engine 102 and task
registry 22 tests passed (counts overlap), including existing independent
closure/choice/phase and domain bag oracles. The new regression test passed
again after counterfactual restoration. These tests do not establish all
requested SQL/grant/merge matrices. No all-repository green claim is made.
After withdrawal the CTE allocation defect remains; the failing new test is
archived with the proposed patch rather than installed without its fix.

All temporary production diagnostics were removed. `git apply --check` accepts
the archived patch on the restored mixed worktree. No broad task kernel,
second optimizer, new priority policy or filter-coalescing patch was shipped.
Further production delivery is blocked on deciding to include the exposed
default-search matching expansion in scope; retaining a 33-second default
regression or hiding it with an earlier stop is not acceptable.
