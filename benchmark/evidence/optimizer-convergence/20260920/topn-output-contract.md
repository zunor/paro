# Native TopN output contract and remaining provider blocker

The first production break is **before publication**, not costing: the native
TopN producer copies ORDER from a canonical semantic template whose projection
map is deliberately erased. In `fulltext_exec_mode_split`, the target group has
one Integer column but the product exposes Integer + Float (hidden rank).
The root contract guard rejects rule 10022 as no-output. Restoring the precise
source occurrence's output columns using the existing binding catalog admits
the TopN without weakening that guard or changing its child/sort operands.

The planner regression uses the real implementation registry, optional rules,
freeze and extraction. It separately asserts publication and output identity;
it does not require an arbitrary VALUES cost winner. The production SQL spill
fixture now selects TOP_N_BUILD / TOP_N_EMIT and executes the expected rows.
Its two new logical_node_id fields are retained (with the existing bijective
ID normalizer), not removed from output. The large OFFSET case remains a
legitimate ordinary-sort fallback. No budgets, priorities or costs changed.

## Second break: provider window, NOT certified fixed

Native staging explicitly supplies no search-provider roots. A bounded local
window experiment (TopN/Projection/Filter/Get only, no Memo representative)
reached and selected FULLTEXT_SCAN TopK. **Execution disproved equivalence**:
on the five-row fixture the scalar scores are `(1,2.375),(2,2.0),(3,2.0)`;
the new TopK LIMIT 1 returned id 3, while ordinary scalar ordering returns 1.
The experiment is withdrawn and archived as
`c2-topn-provider-rejected-experiment.patch`; it is not production code.

The mismatch has a concrete source: scalar fulltext fallback calls
`score_document_from_tokens` without corpus statistics; indexed BM25 calls
`score_document_from_index` with global document frequency and average length.
Equal `FullTextScoreMode::Bm25` enum values do not establish equal SQL ranking.
The new actual execution query is intentionally distinct from EXPLAIN's TopK
coverage assertion. It must return the uniquely highest *scalar* score, id 1.
The historical TopK EXPLAIN remains red until a ranking-equivalent provider
contract is implemented. No snapshot is blessed to hide this missing ability.

This is a C2 blocker, not a performance result. Vector/search/late-fetch
coverage and remaining regress blocks require final independent adjudication.
F2 remains separately unadmitted; QualityPolicySatisfied is not ProofComplete.
