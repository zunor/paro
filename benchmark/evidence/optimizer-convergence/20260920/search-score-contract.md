# Search replacement contract (C2, in progress)

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
