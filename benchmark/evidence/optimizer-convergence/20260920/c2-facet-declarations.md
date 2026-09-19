# C2-3: one authoritative facet declaration through admission

Two minimal failures were reproduced before the change:

1. Initial `RegionForest::normalize` accepts two observations of the same
   facet in disjoint scopes and emits two owners. Incremental Memo upsert
   instead unions the declarations. The same input had different contracts
   depending on its construction path.
2. Deferred facets retained only fingerprints. Rediscovery of a subset forgot
   the original scope and admitted the subset, while the historical dropped
   ID remained in the forest. It was simultaneously admitted and dropped.

The forest now owns each full facet declaration, admitted or deferred. Initial
construction and incremental publication share the checked scope-union and
minimum-priority operation. Contract conflicts are errors, not arbitrary
winner selection. Deferred declarations participate in the next normalization
and in group recanonicalization; savepoint/rollback restores the same Arc.
The old id-only union path is deleted. Admission ceilings are unchanged.

The verifier independently rejects duplicate deferred declarations, required
facets incorrectly deferred, noncanonical scopes and admitted/deferred overlap.
It is not weakened to accept two owners.

## Tests and limits

Before: `facet-minimal-before.log` has 2 deterministic new failures / 4 pass.
After: the same 6 tests pass. Further tests cover merge/readmission/rollback
and conflicting declarations in both input orders. The complete region filter
run has 40 pass / 1 fail; that failure is the unchanged pre-existing RF choice
test. The prior full run has 1324 pass / the same 15 failures as baseline.

Q01's original ownership error is reproduced on the clean F2 parent, with
verification enabled. Full SQL reproduction after this repair is pending.
In particular Q23's unowned physical candidate may have a different lifecycle
cause: it is **not** declared fixed by these tests. Full C2-3 and C4 admission
remain open; this commit closes two specific state-loss defects only.
