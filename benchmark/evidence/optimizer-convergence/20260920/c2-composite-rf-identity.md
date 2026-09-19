# Composite runtime-filter keys are not single-column NDV facts

The first integration Q05 execution after partial-grant error preservation
and server-observation restoration fails with `runtime-filter build-left
candidate lost its domain identity`. The complete failure is retained as
`c0/q05-partial-grants-r2.json` in the private archive. It is not a generic
missing mandatory-class error, nor a proven resource infeasibility.

`join_key_domain_column` correctly returns no single column for a composite
equality key. `expression_cost_facts` incorrectly required that optional
single-column NDV lookup key to construct the build-domain proof identity.
Thus an eligible composite-key RF implementation could not be priced.

The repair separates the two quantities. Single-column statistics remain
optional and no joint NDV is fabricated. The build-domain identity carries
the complete key tuple, its input bindings, canonical group, side and fact
snapshots. Tuple order and duplicate components are retained; semantic
conjunction equivalence alone does not prove identical encoded RF keys.
Both owned and resident fact producers use this same identity construction.
There is no flag, fallback implementation, rule suppression or cost change.

Tests cover distinct composite references through a two-column projection
and a real Memo optimize call, plus identity separation for changed tuple,
tuple order, multiplicity and input layout. The full optimizer suite before
the final distinct-reference fixture refinement reports 1332 pass / the
same 11 unresolved failures. The refined real-engine fixture also passes.
Logs: `rf-composite-full-tests-r2.log`, `rf-composite-distinct-test.log`.

This is one C2 safety slice, not broad SQL certification. Release SQL retest,
the active/deferred facet lifetime failure and the remaining RF selection
assertions are separate obligations; none is blessed by these tests.
