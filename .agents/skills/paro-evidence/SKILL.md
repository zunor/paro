---
name: paro-evidence
description: Design, run or audit controlled Paro benchmark evidence campaigns using benchmark/corpora, including cold compile, first-statement, warm, cross-engine and cost-model comparisons. Use for causal performance or parity claims, not routine engineering gate checks or baseline updates.
---

# Paro comparison evidence

## Worktree and scope first

Resolve the user's selected checkout and inspect HEAD/status before writes.
Preserve mixed staged/unstaged/untracked work. Reuse the agreed isolated source
and immutable dataset seed; never redirect to a hard-coded main checkout,
reset user changes, or create more worktrees/large data copies automatically.
Dirty-source results remain exploratory unless a specifically registered
comparison identifies and controls that source state. Review-only work does not
authorize builds, servers or a new campaign. This workflow never blesses
results, baselines or policies.

## Workflow

1. Confirm the selected checkout, source state and authorized scope.
2. Read CORPORA and the optimizer README's comparison/evidence contracts;
   verify the declared competitor baseline against the actual environment.
3. Register the comparison: EvidenceId, hypothesis, arms, samples, thresholds,
   fixed identities and capacity. Existing-data audits verify the original registration.
4. Collect fresh/interleaved trace-off samples; keep diagnostics separate.
5. Validate complete typed results for every sample outside its timer, not
   only after the entire campaign has finished.
6. Analyze only when the decision rule applies; otherwise report
   Uncovered, Incomparable or NotCertified as appropriate.
7. Archive the bounded campaign manifest, cell timings and single capture
   references through the supported schema; do not pretend a migration is done.

For audit-only requests, inspect existing artifacts without starting collection
or inventing a retrospective registration. Detailed constraints follow.

## Use the maintained harness

Read `benchmark/CORPORA.md`, then the comparison-validity and evidence sections
of `crates/optimizer/readme.md`. Both are available in a standalone clone.
For engineering gates instead, use [paro-benchmark](../paro-benchmark/SKILL.md).
Keep using Paro benchmark; do not write a parallel timer or per-report parser.

Select only the relevant entry points and inspect their live `--help` using
the selected benchmark Python environment before constructing commands:

| Need | Repository entry point |
| --- | --- |
| Paired TPC-DS results, C1 and warm | `benchmark/corpora/tpcds_compare.py` |
| Compile-only diagnosis, not SELECT C1 | `benchmark/corpora/cold_planning.py` |
| First-execution diagnosis | `benchmark/corpora/d6_execution_profile.py` |
| Known-cardinality quality checks | `benchmark/corpora/plan_quality.py` |
| Offline stage attribution | `python -m benchmark.corpora.first_statement_attribution` from the repository root |

Inspect `benchmark_evidence.py` for source/build/seed identities and process
lifecycle, and `tpcds_result_contract.py` for output schema, numeric, bag and
ORDER contracts. These are shared libraries, not interchangeable CLI tools.
Do not assume another branch's flags/schema are installed or silently upgrade
dependencies to obtain them. The full correctness-corpus sequence in CORPORA
applies when claiming that gate, not as an extra prerequisite for every pilot.

## Register the comparison before confirmatory collection

Commit a bounded registration with EvidenceId/content hash: hypothesis,
intervention, arms, fixed query cases, independent sampling unit, run order,
sample sizes/power, numeric decision thresholds, exclusions, uncertainty,
resource envelope and storage limits. Known pilots inform registration but
cannot be retroactively called preregistered. Amendments preserve the original
and require new confirmatory samples where the decision changes.

Pin source commit/dirty content, binary/build settings, harness/SQL/schema/data
seed, optimizer-visible metadata, OS/architecture, DOP, memory, actual grants,
protocol, timer boundaries, verification and instrumentation. Pin competitor
version **and** package/binary and extension hashes/settings; upgrades create a
new comparison, not an inherited parity claim. Check the installed harness's
effective `optimizer_verify` setting: some versions force it on. If a requested
mode is unsupported, disclose it; do not invent a disabling flag.

The competitor declaration starts at the selected checkout's
`benchmark/requirements.txt`, supplemented by the referenced runtime manifest
and campaign registration **when actually present**. Follow
[Declared competitor baseline](../../../benchmark/CORPORA.md#declared-competitor-baseline):
check the selected Python/worker's imported package, native engine and extension
identities against those declarations before collection. A requirement range
is not an exact baseline, and a version pin does not declare binary/extension
hashes. Missing declarations or mismatches stop confirmatory collection; do not
bless whichever version happens to be installed or change dependencies without
authorization. Do not assume a runtime-manifest filename exists on this branch.

## Collect comparable samples

- Use independently owned processes, ports, test-data/temp paths and explicit
  output destinations. Preserve immutable seeds; inspect setup/build/lifecycle
  behavior before invoking a collector. Do not clean another task's processes.
- C1 is target occurrence 0 in a fresh process with verified target cache miss,
  not simply the first SQL statement issued to that process. Warm and other
  registered cache regimes are separate cohorts. Do not pre-touch or precompile
  a normal cold target; label intentional pre-touch an intervention.
- Use the registered same-batch interleaving/rotation and resource envelope.
  Resample at the actual independent process/block level, not every repeated
  timing as an independent process. Do not launch competing performance runs
  on one host unless resource separation/interference is part of the design.
- Normal samples are trace-off; diagnostic samples are separate and never
  certify speed. Do not subtract times from different cohorts to manufacture
  a same-statement breakdown. Rendering/transport/admission boundaries and
  observer overhead must remain explicit.
- Validate complete typed results, multiplicities and required ORDER outside
  timing; an independent bounded numeric certificate must be explicit, never
  an ad-hoc epsilon. EXPLAIN success or matching fingerprints is not SQL result
  validation. Preserve failures, timeouts, all valid slow samples and exclusions.
- Associate diagnostic structure only with matching per-sample input and
  compile/admission receipts under the repository contract. Missing/mismatched
  receipts block that joint explanation, not retention of normal timings.
  Historical receipts cannot be reconstructed from a similar plan's hash.

## Analyze only a valid comparison

First establish that the decision rule is applicable to these measurements.
Do not use unrelated report ratios as causal speedups, uncalibrated cross-grant
raw costs as one ranking, fingerprints as equivalence proofs, or undersampled
rank correlation as certification. Keep Pareto dominance/incomparability,
objective selection and runtime admission separate. Label selection coverage,
uncertainty, effective policy and stopping state; QualityPolicySatisfied plus
SearchIncomplete is not ProofComplete. A median below a target is not a gate
pass without the registered uncertainty/tail/coverage requirements.

Use maintained schema readers/validators. If required evidence is absent,
report Uncovered/Incomparable/NotCertified as applicable rather than create a
one-off JSON extractor or fill missing fields with zero.

## Bound and retain evidence

The target compact archive shares campaign `manifest.json` and `README.md`,
stores samples/receipts in each `(QueryCase, ArmId)` cell's `timings.json`, and
references each `EXPLAIN (COMPILE)` capture once. Read the exact registered
volume formula and limits in `crates/optimizer/readme.md`; do not invent a
per-query arm, duplicate manifests, gzip to pass a quota, or delete slow samples
to fit it. Raw events and `.parod.log` are not ordinary campaign evidence.

This schema/SQL migration is a design target until implemented and validated
in the selected checkout. Do not fabricate EXPLAIN fields, claim the current
collectors enforce its limits, or copy giant legacy reports into a new archive
format unchanged. Keep existing necessary artifacts under explicit ownership;
missing bounded collection is an implementation gap, not permission to build a
new exporter or discard the only correctness reproducer. Run-scoped paths also
need source/attempt isolation, not just a unique top-level report filename.

Stop further confirmatory collection on result errors, identity drift,
uncontrolled resource interference, missing required evidence or exhausted
registered capacity; preserve the reason and partial samples. Do not rerun
until green, tune thresholds to observed data, bless expected files, or delete
history without separate authorization. Deliver reproducible manifests,
commands, validation and a conclusion proportionate to the evidence.
