# Transformation binding migration

All planner transformations consume `PatternBinding` operands. A binding
selects every inspected logical expression explicitly and records each Memo
frontier revision read during enumeration. The planner-plan instance used by
the mature rewrite kernels is reconstructed from that exact binding; no rule
chooses a representative expression.

| Rule | Root shell | Bound scope | Preserved group boundary |
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
