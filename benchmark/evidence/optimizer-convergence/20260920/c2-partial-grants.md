# C2-2: partial verified grant availability

Contract: declaration of a resource class is not an executable plan for it.
The old all-mandatory-class assertion is removed. Available classes retain
their original verified FrozenCandidate. A declared class without an image
is recorded as unresolved, not ProvenInfeasible; neither admission nor a
fallback can use it. The coverage verifier checks the correspondence in both
directions. Reduction of actual available resources returns a resource error
if none of the verified variants fits.

Mandatory and optional implementation/verifier failures propagate unchanged.
Missing candidates are represented as data (possibly an empty class search),
not exceptions that could accidentally swallow an internal error or an
unsupported implementation. No infeasibility proof is manufactured from an
empty frontier. The public boundary emits SQLSTATE 53400 when no executable
image was obtained; the error explicitly says infeasibility is not proven.

This is a correctness change, not a no-op or a performance claim. It does not
admit the isolated F2 deferred-image implementation, change class resources,
or change admission's objective ordering.

## Validation

Real-engine cases cover missing class 0, missing expected class 2, partial and
empty portfolios in direct/optional modes, exact image admission, resource
shrink, class sharing, deferred obligations, and cancellation/rollback. An
injected internal failure and an injected implementation resource failure in
both mandatory and optional phases must retain their SQLSTATE and message.
Coverage tests reject both a missing image falsely marked available and a
verified image falsely marked unresolved.

The previous `lazy_grants_cannot_publish_a_partial_mandatory_portfolio` test
asserted the contract now expressly rejected by design §3.4. It is replaced by
stronger engine-to-admission assertions; unrelated red expectations are not
changed. Full optimizer suite at this step: 1321 passed, the same 15 failed
as the fixture-repaired baseline (1319 passed). The additional metadata test
is validated in the following full run: **1322 passed / the same 15 failed**.
The final error-handling simplification passes all 9 grant-lazy engine tests.

Logs: private recovery archive `c0/partial-grant-full-tests.log`,
`c0/partial-grant-final-tests.log`, and `c0/partial-grant-final-targeted.log`.
SQL/regress/broad and performance gates remain **NotCertified**;
the historical 20-query mandatory-class failure set is not declared resolved
by unit tests alone. ProvenInfeasible/Unsupported evidence classification for
broader search and remaining current errors still needs follow-through.
