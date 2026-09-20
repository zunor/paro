# C2: empty task responses are not infeasibility proofs

The engine formerly emitted `TaskOutcome::Infeasible` whenever it had no
winner outside a readiness pass, including a yielded or budget-limited
prefix. A second `infeasible_goals` set duplicated this unproven conclusion.
Both are removed. `NoCandidate { cursor }` describes only the consumed
evaluation, with its existing exact goal, ReadSet and resumable cursor.
Completion of that evaluation is not proof that no implementation exists.
The resident implementation phase still determines whether mandatory work
covers a subsequent optional request; the physical proof domain now also
includes the phase. No budget or stop policy is changed.

`TaskRegistry::fail` formerly discarded the supplied cause and manufactured
a SearchCandidate resource stop. It now retains the cause as `Failed`.
Engine `Result` errors, including cancellation, still propagate unchanged;
the registry detail is not a replacement SQL error authority.

The engine does not currently construct a proof of resource infeasibility
or lack of implementation capability. Therefore an empty grant remains
unresolved through `GrantSearchCoverage`, and admission can only select a
verified available image. This change does not pretend to deliver a new
ProvenInfeasible or Unsupported certificate. Existing bound certificates
retain their exact search-domain/goal/ReadSet/grant conditions.

Validation: `cargo test -p paro-optimizer --lib --locked`: 1347 passed,
zero failed. New tests exercise an empty mandatory prefix followed by a
feasible optional implementation (without logical publication), repeated
reuse, phase-distinct proof domains and retention of failed-task causes.
Existing partial-grant/resource-shrink, yield, cancellation/rollback,
group-merge, fact-invalidation and source-sensitive RF tests also pass.
Log: private archive `c0/c2-task-outcomes-test-r2.log`.

This is not yet the C2 integrated SQL or performance gate. F2 remains
isolated; no missing historical build evidence is repaired by these tests.

The additional planner regression uses real cross-product implementation,
costing, freeze and extraction with a frozen statement context. Expected
class 2 has no verified image; class 0 remains executable and class 2 is
explicitly unresolved. This is not a custom Leaf registry. The first new
fixture accidentally selected the standalone eager API (which has no lazy
coverage metadata); adding the statement context selects the intended
production boundary without weakening any assertion. Both attempt logs are
retained as `c2-real-planner-partial-grant{,-r2}.log`.
