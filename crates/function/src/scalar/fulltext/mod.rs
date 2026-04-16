// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Full-text search functions.

mod fallback;
mod headline;
mod matching;
mod query;
mod rank;
mod tokenize;

pub use headline::*;
pub use matching::*;
pub use query::*;
pub use rank::*;
pub use tokenize::*;

use crate::ScalarFunctionSet;

/// Register all full-text functions.
pub fn register_fulltext_functions() -> Vec<ScalarFunctionSet> {
    vec![
        rank::get_bm25_functions(),
        matching::get_fulltext_match_functions(),
        rank::get_bm25_score_internal_functions(),
        matching::get_fulltext_match_internal_functions(),
        tokenize::get_to_tsvector_functions(),
        query::get_to_tsquery_functions(),
        query::get_plainto_tsquery_functions(),
        query::get_phraseto_tsquery_functions(),
        query::get_websearch_to_tsquery_functions(),
        rank::get_ts_rank_functions(),
        rank::get_ts_rank_cd_functions(),
        headline::get_ts_headline_functions(),
    ]
}
