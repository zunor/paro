---
name: paro-evidence
description: "Design or audit formal Paro performance claims: release acceptance, cross-engine parity, model calibration certification or non-inferiority. Not for routine exploration; use paro-benchmark there."
---

# Paro formal evidence

Daily iteration uses [paro-benchmark](../paro-benchmark/SKILL.md).
Use this workflow only for a requested formal claim or its audit. Review-only
work does not authorize new runs. Never bless results or evolve a policy here.

## Register, collect, decide

1. Inspect the selected checkout and existing evidence. Before confirmatory
   sampling, version a short registration: hypothesis/intervention, arms,
   independent sampling unit, sample size/power, numeric thresholds, exclusions,
   uncertainty, coverage/held-out rules, resources and finite storage budget.
   Existing pilots cannot be retrospectively preregistered.
2. Pin source/dirty content, binary/build, harness, SQL/schema/data/metadata,
   protocol, cache regime, DOP/memory and actual verification/observers.
   Follow [declared competitor baseline](../../../benchmark/CORPORA.md#declared-competitor-baseline):
   requirements constrain the version; the registration also identifies actual
   native binaries/extensions/settings. Upgrades create a new comparison.
3. Use maintained collectors with balanced/interleaved blocks. Treat the fresh
   process/block as the independent unit when appropriate, not each repeated
   warm timing. Normal measurements and diagnostic captures remain separate.
4. Validate complete types, multiplicities and required ordering outside timing.
   Keep every failure, retry, valid slow sample and exclusion. Explicitly
   bounded independent numerical certificates are not strict equality.
5. Evaluate the registered rule, including uncertainty/tails/coverage. A small
   median alone is not certification. Report missing evidence or confounding
   as Uncovered/Incomparable/NotCertified, not zero or an invented receipt.

## Comparison validity

- Do not infer causal speedups from unrelated report ratios or sums/differences
  of cohort medians. A decomposition needs direct same-lifecycle observations.
- Match inputs, admitted plan/resources, node/port/phase and units. Equal plan
  fingerprints locate artifacts; they are not semantic equivalence proofs.
  A diagnostic mismatch blocks joint attribution, not preservation of valid time.
- Model comparisons must share an applicable objective/fact/resource context.
  A partial order has incomparable candidates: rank correlation is descriptive,
  not a pruning proof. Use independent small-region enumeration, measured
  dominance violations and selection regret for the decisions actually made.
- Changing a threshold/exclusion after seeing outcomes requires a disclosed
  amendment and fresh confirmation where it changes the decision.

## Bounded local evidence, small durable decisions

Keep raw runs in ignored `benchmark/runs/<run-id>/` or a declared external root,
not in Git. Share campaign identities, retain samples/receipts per query/arm
cell and reference each bounded compile capture once. Use the actual typed
RunOutput/validator limits; don't duplicate a capacity formula in prose.
Register a finite total budget and stop collection when exhausted; don't
discard slow samples or gzip an oversized event stream to pass a limit.

Preserve the complete raw run through review. If long-term reproducibility
needs retention, explicitly name an approved artifact location, checksum and
retention owner; without retained raw data, a historical decision is not a
currently re-auditable certification. Git normally gets at most a page of
conclusion/registration with identities, uncertainty, commands and limitations.
Correctness fixtures belong in maintained tests/corpora, not historical reports.

Stop confirmatory collection on result errors, identity drift, uncontrolled
interference or missing required evidence. Report partial results honestly.
Neither this workflow nor a successful check authorizes baseline changes,
history rewriting, automatic deletion, or publication.
