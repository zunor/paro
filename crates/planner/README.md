# Paro planner

This crate owns SQL binding and the plan contracts shared by optimization and
execution. Binding and plan representation stay together; there is no separate
binder or plan crate.

## Ownership

| Module | Responsibility |
| --- | --- |
| `binder/` | Names, scopes, types, parameters and bound statement semantics |
| `expression/`, `operator/`, `plan/` | Bound expressions, logical operators, plan arenas and column identities |
| `physical/` | Executable topology, typed operator payloads, dependencies, canonical identity, properties and resource/admission contracts |

The optimizer constructs physical plans. Execution consumes the finalized
contracts to admit work, lower executable images and run operators. Neither
consumer needs to own another copy of the plan types.

Physical cost values and their composition/comparison laws are shared data
contracts. Calibrated operator formulas, cardinality estimation, candidate
enumeration, Memo identities and task scheduling belong to `paro-optimizer`,
not here. `physical/scalar_identity.rs` encodes expression semantics without
constructing Memo scalar objects. Query-local interning stays with its owner.

## Boundaries

- No production dependency on optimizer or execution, directly or transitively.
  `make plan-boundaries` checks normal/build dependencies, including optional,
  target-specific and aliased edges. Test fixtures are a separate boundary.
- Do not change canonical bytes because a type moved or a display name changed.
  A plan identity includes its executable semantics; allocation-local ids and
  Debug formatting are not stable encodings. Equal hashes do not prove SQL
  equivalence.
- Preserve NULLs, duplicates, output names/types/order, observable evaluation
  and error boundaries when composing plan contracts. Both optimizer rewriting
  and execution lowering use the same guarded projection composition.
- Resource feasibility, actual dependencies, write isolation and executable
  capability checks remain production checks. Moving the verifier here does
  not make these debug-only checks or authorize bypassing admission.
- Keep fallible construction and invalid-graph errors explicit. Published
  artifacts, admitted operating points and executable images are distinct
  lifecycle states; a physical-plan type does not imply completed execution.

The [optimizer guide](../optimizer/readme.md) describes stage ordering and
diagnostic/evidence contracts. Actual Cargo edges are defined by each manifest,
not by the order of the query lifecycle.
