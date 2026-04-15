//! Full-text search functions.

mod eval;
mod highlight;
mod pg;
mod score;

pub use highlight::*;
pub use pg::*;
pub use score::*;

use crate::ScalarFunctionSet;

/// Register all full-text functions.
pub fn register_fulltext_functions() -> Vec<ScalarFunctionSet> {
    vec![
        get_bm25_functions(),
        get_fulltext_match_functions(),
        get_bm25_score_internal_functions(),
        get_fulltext_match_internal_functions(),
        get_to_tsvector_functions(),
        get_to_tsquery_functions(),
        get_plainto_tsquery_functions(),
        get_phraseto_tsquery_functions(),
        get_websearch_to_tsquery_functions(),
        get_ts_rank_functions(),
        get_ts_rank_cd_functions(),
        get_ts_headline_functions(),
    ]
}
