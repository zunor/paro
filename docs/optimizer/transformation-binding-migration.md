# Transformation binding migration

All planner transformations consume `PatternBinding` operands. A binding
selects every inspected logical expression explicitly and records each Memo
frontier, logical-fact, and statistics snapshot read during enumeration. The
semantic-plan instance used by the mature rewrite kernels is a boundary view
reconstructed from that exact binding; no rule chooses a representative
expression. Its output is imported transactionally as canonical operator
shells and explicit child groups.

| Rule | Root shell | Bound scope | Preserved output boundary |
|---|---|---|---|
| expensive predicate placement | Filter | exact subtree binding | none |
| CTE partitioned materialization | MaterializedCTE | exact producer/consumer binding | sharing facet |
| CTE inline | MaterializedCTE | exact producer/consumer binding | discharged sharing input |
| CTE demand pushdown | MaterializedCTE | exact producer/consumer binding | sharing facet |
| CTE filter pushdown | MaterializedCTE | exact producer/consumer binding | sharing facet |
| join region enumeration | ComparisonJoin | exact connected-region binding | region owner |
| aggregate post reduction | MaterializedCTE/Projection/Filter | exact subtree binding | none |
| MARK to SEMI | any carrier shell | exact consumed-marker binding | none |
| join elimination | unary relational shell | exact outer-join binding | none |
| aggregate join preaggregation | Aggregate | exact join binding | none |
| aggregate join subsumption | Aggregate | exact aggregate/join binding | none |
| aggregate non-null input | Aggregate | exact input binding | none |
| aggregate dimension deferral | Aggregate | exact join-region binding | region owner |
| aggregate input materialization | Aggregate | exact input binding | none |
| TopN introduction | Limit | exact Limit/Order binding | Order input group |
| limit pushdown | Limit | exact projection binding | none |
| late payload fetch | Projection/Aggregate/TopN | exact selective subtree binding | none |
| scalar aggregate window | join/unary carrier | exact aggregate binding | none |

The table is exhaustive over `PlannerTransformation::ALL`. “None” means the
rewrite changed that subtree and staging owns the newly emitted shells; it
does not mean an arbitrary representative was substituted. TopN retains the
`Order` input group because `Limit(Order(G)) -> TopN(G)` changes only the two
operator shells above `G`.

Removed search bridges:

- recursive representative selection by `Initial` proof or rule id;
- hand-authored semantic dependency lists;
- fingerprint tie-breaking to choose a logical input;
- the TopN-only direct input-group escape hatch;
- pre-match traversal of the complete descendant closure.

`PatternEnumerationCompletion` reports `Complete` or `BudgetLimited`. Binding
and child-frontier enumerators both retain one explicit omission witness, and
the mandatory physical baseline remains available when optional search is
limited.

Rules that publish a complete local frontier for an observed binding declare
that contract through `output_saturates_observed_binding`. Produced
expressions inherit only the corresponding read cursor. This prevents a
whole-region enumerator from immediately enumerating its own complete output,
while a later child alternative, fact change, or statistics snapshot change
invalidates the cursor. Ordinary chain rewrites keep the default non-saturating
contract and may continue rewriting their own output.

The remaining `instantiate_bound_plan` helper is deliberately not a search
bridge. It accepts a fully explicit `PatternOperand` tree, fails on an
unconsumed group hole, and has no API for selecting an expression from a
group. It is the typed semantic boundary for the existing rewrite kernels;
winner extraction and positional layout conversion remain separate.
