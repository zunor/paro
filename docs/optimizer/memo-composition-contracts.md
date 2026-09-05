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
- Parent recipes enumerate every admitted child-frontier product and bind
  exact immutable `CandidateId` values. Candidate storage is append-only, so
  frontier ordering, dominance pruning, and group merging cannot retarget an
  already recorded winner tree.
- Logical frontier revisions are globally monotonic across insert, rollback,
  and group merge. A rolled-back alternative can therefore never reuse an
  observed read cursor.

## Transformation bindings

- A transformation consumes an explicit operator shell, selected nested
  shells, scalar identities, and group holes. It never selects a representative
  tree by insertion order, rule id, or payload fingerprint.
- Matching records the precise logical frontier and fact revisions it reads,
  including reads which currently produce no match. A later alternative or
  fact revision invalidates only dependent bindings.
- A rule which publishes the complete output frontier for one observed binding
  may seed its outputs with that binding's read cursor. The cursor is an
  incremental-work optimization only: any observed frontier, fact, or
  statistics change makes those outputs ordinarily matchable again.
- Mature planner rewrite kernels receive a temporary semantic plan rebuilt
  from the exact `PatternBinding`. This boundary adapter cannot choose a Memo
  representative. Rewritten output is decomposed into canonical operator
  shells during transactional staging; declared group holes are preserved as
  group references.
- Equivalence proofs are audit evidence checked at publication. They never
  participate in root dispatch, binding identity, or incremental idempotence.

## Execution phases

- Streaming operators inherit executable task supply from their input
  pipeline. Only a source, exchange, or pipeline breaker creates supply.
- Build, probe, local merge, global merge, and finish are distinct phases.
  Coordination cost is charged once for each scheduler-visible phase.
- Source filtering replaces raw work before phase folding. Total work, span
  lower bound, and estimated duration remain distinct quantities.
- `max_parallel_tasks` records the operating point of all phases contained in
  a candidate; `output_pipeline_tasks` is the executable supply exported to a
  streaming parent. A wide projection cannot manufacture workers when its
  source pipeline exported one task.
