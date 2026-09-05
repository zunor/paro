# Memo composition contracts

This document records the invariants shared by Memo exploration, physical
cost composition, and extraction. They are execution contracts, not tuning
heuristics.

## Source work and runtime-filter domains

- `WorkSourceId` identifies one query-local base-row source. A source lane
  carries its immutable pre-filter row count independently of its byte and CPU
  cost.
- `DomainProofId` identifies the build domain, probe-key mapping, equality and
  NULL semantics, and statistics/proof snapshot used to derive retention. The
  same domain proof is idempotent.
- `EvaluationOccurrenceId` identifies one physical predicate evaluation. More
  than one occurrence may publish the same domain proof. Every retained
  occurrence is charged, but the proof changes the survivor prefix once.
- Predicate-application cost is attributed by immutable input rows, never by
  source byte cost or already-filtered cost. Normalized predicate order and
  lane partitioning therefore cannot change total work.
- Expected intersection estimates may combine distinct proofs. Conservative
  upper bounds are absolute in the original source domain and combine by
  minimum unless a joint-domain proof explicitly establishes correlation.

## Search completion

- A mandatory baseline is always considered without consuming optional
  search budget.
- Every omitted logical binding or physical child combination produces a
  `BudgetLimited` witness. A finite-budget winner without that witness is a
  claim that the applicable search space was completely enumerated.
- Parent recipes bind exact immutable child candidate identities. Frontier
  ordering and pruning cannot change an already recorded winner tree.

## Transformation bindings

- A transformation consumes an explicit operator shell, selected nested
  shells, scalar identities, and group holes. It never selects a representative
  tree by insertion order, rule id, or payload fingerprint.
- Matching records the precise logical frontier and fact revisions it reads,
  including reads which currently produce no match. A later alternative or
  fact revision invalidates only dependent bindings.
- Transformation output is staged as Memo shells plus group references; plan
  materialization is not part of exploration.

## Execution phases

- Streaming operators inherit executable task supply from their input
  pipeline. Only a source, exchange, or pipeline breaker creates supply.
- Build, probe, local merge, global merge, and finish are distinct phases.
  Coordination cost is charged once for each scheduler-visible phase.
- Source filtering replaces raw work before phase folding. Total work, span
  lower bound, and estimated duration remain distinct quantities.
