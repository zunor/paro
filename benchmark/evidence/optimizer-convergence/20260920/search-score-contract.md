# Search replacement contract (C2)

This replaces the incompatible use of one `Bm25` identity for two scoring
functions. The five-row counterexample and the withdrawn implementation remain
archived. No cost, search budget, stopping policy, or chain default is changed.

## Score identity and scope

`DocumentRankV1` is the existing two-value SQL rank algorithm (k1=1.2, b=0.75,
document length as its own average length, no corpus IDF). `CoverDensityV1`
remains document-local. `CorpusBm25V1` uses the query's pinned visible table
corpus, including transaction visibility and uncovered rows, before any query
filter. Its statistics frame is derived from and bound to that exact MVCC and
overlay lease, not index-generation row counts. An unbound caller-supplied
global-statistics parameter is removed. The fulltext intent retains query
kind, query, normalized tokenizer configuration, and versioned algorithm;
provider opening checks the generation's tokenizer contract. No scalar score
changes because an index, another document, or compaction appears.

The physical request fingerprint includes this intent. Extraction validates the
selected provider intent against the actual logical score expression and sort
direction. `Exact` alone is not an equivalence proof. An index TopK replacement
requires the matching fulltext predicate, descending score, no offset, and one
ordering key. Score-only/ascending/multiple-key/offset shapes retain ordinary
TopN. Casts and foreign column bindings do not acquire an implicit score proof.
Residual predicates are evaluated before provider truncation.

## Publication

Arena and Native staging use the same bounded borrowed operator window:
Filter/Get or TopN/Projection/Filter*/Get. Child group holes stay opaque; there
is no representative selection, recursive owned export, or whole-Memo copy.
Unsupported shapes return no candidate; reader errors propagate. Existing
staging transactions, cancellation/control checkpoints, capability tokens,
resident scalar/layout contracts and physical implementation publication remain
the authority. The generated owned value is a leaf executable payload, not an
export of its input subtree.

## Visibility before truncation

Fulltext overlay deletes use visible exact scoring before TopK. Vector search
now puts overlay visibility in its existing exact row-set admission contract
before either segment or partition TopK. A real indexed exact vector query
previously returned no rows after deleting its nearest neighbor in a transaction;
the next visible neighbor is required. ANN remains explicitly approximate:
the existing result guarantee and search objective are not upgraded by reranking.

## Validation status

The first current-binary regress run (`edd872e9`, raw report retained as
`c2-search-regress-report`) is 176 pass / 9 fail. The three historical fulltext
TopK failures are closed without changing their expected plans. Vector-search
blocks 22, 23 and 36 select the existing adaptive provider instead of a forced
index-source wrapper: request, distance, exact objective, predicate row-set,
query parameters and all result blocks agree. Only the wrapper and its explicit
`Strategy: adaptive` line are revised; no exact/ANN or filter guarantee is
removed. The fallback scan's retained `category` column in
`pgvector_topn_filter_flow` remains a separate late-materialization review,
not a reason to force an old plan or bless that file.

Integration also exposed unsupported scalar residuals at two provider roots.
Both TopK and bitmap-filter publication now use the existing predicate-template
contract to decline unsupported replacement **before selection**. The real SQL
`length(content)` counterexample remains a required passing execution test.
The initial failing workspace log and binary remain archived; a final clean
rebuild and gate rerun are required after these fixes.

Preflight (not final clean-binary certification): 91 storage fulltext tests and
11 session fulltext/search tests pass. The latter execute the real SQL planner,
provider, compaction, transaction insert/delete and rollback paths. The new SQL
regression checks exact scores, membership, hidden ordering, LIMIT/OFFSET,
secondary keys and NULL, and separately checks FULLTEXT_SCAN execution.

Initial fulltext preflight: 12/12 pass. Vector preflight: 6/8 pass; remaining
differences are an adaptive-provider EXPLAIN and a pre-existing fallback scan
column layout, not yet adjudicated here. These dirty-binary preflights are not
substitutes for final integration, full regress or current-binary corpus checks.
Final manifests and gate results will be recorded separately. C2 is not closed;
F2 remains independently unadmitted. No performance claim is made.

## Final implementation and independently checked boundaries

The final production revision is `4b1d5714`; executable and source hashes are
recorded in `search-validation-manifest.json`. The earlier `d5f73817` binary
(SHA-256 `1787994b266a32afe35888935b2ca34880243f7802da1a5640935009e59ee5e3`)
and its partial corpus remain intermediate evidence, not final certification.

Dense vector score casts are not identity operations. The SQL counterexample
`CAST(distance AS INT)` previously returned `Float(0.25)` after replacement.
The matcher now declines outer score casts for dense and sparse intents, as it
already does for fulltext; ordinary TopN retains the actual conversion and
output type. The SQL counterexample passes. Dense scalar distance currently
treats a NULL vector as a zero vector: no-index, indexed, and LIMIT 1 probes
confirm this existing behavior. This work does **not** silently replace that
logical contract with NULL propagation.

A second production counterexample exposed the raw score-port type mismatch:
projected dense distance was declared DOUBLE but materialized FLOAT. Physical
extraction now declares the provider's actual FLOAT source and uses the existing
typed, lossless FLOAT-to-DOUBLE cast in ordinary Projection. Both logical score
values and runtime value types are checked through SQL. This is an extraction
contract repair, not a distance algorithm or executor change.

Sparse SQL coverage is explicitly **fallback**, not provider certification.
Storage accepts binary Blob sparse row images; the old SQL provider matcher
recognizes only Varchar. The new real SQL fixture creates a Blob index, checks
ASC and DESC (including zero-overlap rows), and checks that no sparse/adaptive
source executes. This is not evidence that the storage sparse TopK implements
the entire SQL score domain. No ANN reranking or sparse index existence is
accepted as an exact SQL replacement proof.

CoverDensity's five-row fixture has three peers, all scoring 2.0. The first
new test incorrectly assumed row 1 was uniquely best, copying the document-rank
expectation; raw r3 regress evidence is retained. Independent no-index and
indexed executions returned identical `(id, score)` multisets. The corrected
test checks exact scores, deterministic secondary ordering separately, and
real FULLTEXT_SCAN execution without imposing an arbitrary peer identity at
LIMIT 1. The original rank counterexample still has a **unique** best row 1.

## Integration gate ledger

All artifacts below are in
`/Users/linjunhong/paro-convergence-archive/20260920/c0/`.

- `c2-search-full-regress-final.log`: **177 pass / 8 fail**, all 185 cases run,
  optimizer verification enabled, FD limit 65536, serial server experiments.
  DocumentRank and CoverDensity execute FULLTEXT_SCAN; hidden TopN ordering and
  spill/fallback cases pass. The three old fulltext failures close without
  changing their expected files.
- `c2-search-regress-final2-comparison.json`: the eight remaining actuals are
  byte-identical to r4 (and r2). This establishes attribution, **not acceptance**.
  Their 25 differing blocks remain unadjudicated EXPLAIN contracts:
  `agg_join_subsumption`, `agg_singleton_groups`, `explain_analyze`,
  `explain_basic`, `join_explain_advanced`, `rowset_scan_pushdown`,
  `statistics_query`, and `pgvector_topn_filter_flow`.
- Vector snapshot adjudication changes only blocks 22/23/36 of
  `vector_search.result`: the selected adaptive exact source replaces the
  index wrapper; request, distance, filtering, result guarantee, and rows are
  retained. The separate fallback `category` layout is **not** blessed.
- `c2-search-session-r6.log`: 13 real SQL tests pass, including dense overlay
  deletion before truncation, score casts, sparse fallback, fulltext tail,
  compaction and transaction rollback. This does not substitute for corpus.
- Benchmark Python tests: 187 pass plus 9 independent-oracle tests; regress
  harness: 101 pass / 1 existing skip. The unchanged typed-result-v4 and Q39
  contracts are not weakened.
- `c2-search-preserve-*-final.json`: main 133, docs 17, other isolated tree 57
  pre-existing files verified unchanged. No user files or unique evidence were
  removed.

Final workspace/corpus summaries and the clean-source manifest are recorded
in the validation manifest accompanying this document. The interrupted
`c2-search-corpus/` batch and the 198 historical captures cannot certify this
binary. Normal performance was not run. C2 remains blocked by unresolved
regress contracts; F2 remains separately unadmitted. Search completion is not
inferred from a successful quality handoff, and no ProofComplete or parity
claim is made.
