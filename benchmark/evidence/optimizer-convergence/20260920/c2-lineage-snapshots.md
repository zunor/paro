# Selective lineage snapshot adjudication

Eight expected transcripts are updated for the existing logical-node lineage
rendering contract. This is not acceptance based on two arms failing alike.
For each entire transcript, removing **only the newly added text field** from
the archived actual reproduces the old expected byte for byte. Thus operator
roles, topology, row counts, spill behavior and SQL result values are unchanged.
The rendering source is `execution/src/explain/analyze_render.rs`.

The acceptance normalizer does **not** remove that field. It alpha-renames
allocated IDs in text and JSON while preserving presence, first occurrence,
shared identities and role assignments. Negative tests reject missing IDs,
incorrect sharing, wrong references and changed roles. All 126 transcript blocks
in these eight cases pass the revised comparator against the archived actuals.

| Case | Blocks | Decision |
| --- | ---: | --- |
| graph/graph_explain | 32 | Add lineage; same graph modes, rows and topology |
| python_udf/explain/runtime_bridge | 5 | Add lineage; same bridge/operators/results |
| query/aggregate/select_aggregate_spill | 17 | Add lineage; unchanged spill and aggregate contracts |
| query/cte/cte_explain | 4 | Add lineage; preserve separate consumer and recursive identities |
| query/join/join_spill_external | 25 | Add lineage; unchanged build/probe and external paths |
| query/property_repair_runtime | 2 | Add lineage; same preserved-side and property repairs |
| query/select/select_order_by_spill | 22 | Add lineage; same external ordering |
| system/spill_observability_guardrails | 19 | Add lineage; preserve guardrail observability |

Original expected hashes and all original actuals remain in
`c2-regress-gate.json` and the archived paired reports. A new full regress run
before this update reproduced 157 pass / 27 fail in 78.51 seconds on the clean
production binary recorded in `c2-source-manifest.json`, verifier on, FD 65536,
four threads and 2 GB. The raw report is separately archived as
`c2-v3-regress-before-expected-report`; it is not overwritten.

No TopN/search-provider, aggregation, resource-envelope or settings snapshot is
accepted by this commit. In particular, a test intended to exercise TopK must
not be converted to a full-sort test just because both executions return the
same rows. Those remaining failures still block C2.
