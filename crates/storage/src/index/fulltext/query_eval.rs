// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared query evaluation for full-text match, headline, and scoring.

use std::collections::BTreeMap;

use super::query_parser::ParsedQuery;
use super::tokenizer::{SpannedToken, Token, TokenPosition};

/// Borrow-only token view used by query evaluation.
pub trait TokenLike {
    fn term(&self) -> &str;
    fn position(&self) -> TokenPosition;
}

impl TokenLike for Token {
    fn term(&self) -> &str {
        self.term.as_str()
    }

    fn position(&self) -> TokenPosition {
        self.position
    }
}

impl TokenLike for SpannedToken {
    fn term(&self) -> &str {
        self.term.as_str()
    }

    fn position(&self) -> TokenPosition {
        self.position
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchExtentKind {
    Term,
    Prefix,
    Phrase,
    Proximity,
}

impl MatchExtentKind {
    pub fn weight(self) -> u32 {
        match self {
            Self::Term | Self::Prefix => 1,
            Self::Phrase | Self::Proximity => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchExtent {
    pub start_pos: TokenPosition,
    pub end_pos: TokenPosition,
    pub kind: MatchExtentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryStats {
    pub positive_lexemes: usize,
    pub has_prefix: bool,
    pub has_proximity: bool,
    pub has_phrase: bool,
    pub clause_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryMatch {
    pub matched: bool,
    pub extents: Vec<MatchExtent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeMatch {
    pub(crate) start_pos: TokenPosition,
    pub(crate) end_pos: TokenPosition,
}

pub fn matches_query<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> bool {
    evaluate_query(tokens, query).matched
}

pub fn collect_match_extents<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> Vec<MatchExtent> {
    let result = evaluate_query(tokens, query);
    if result.matched {
        result.extents
    } else {
        Vec::new()
    }
}

pub fn query_stats(query: &ParsedQuery) -> QueryStats {
    let mut stats = QueryStats::default();
    accumulate_query_stats(query, &mut stats);
    stats
}

pub fn positions_following_by_distance(
    left_positions: &[TokenPosition],
    right_positions: &[TokenPosition],
    distance: u32,
) -> Vec<TokenPosition> {
    let mut i = 0usize;
    let mut j = 0usize;
    let mut out = Vec::new();

    while i < left_positions.len() && j < right_positions.len() {
        let Some(target) = left_positions[i].checked_add(distance) else {
            break;
        };
        let right = right_positions[j];
        if right < target {
            j += 1;
        } else if right > target {
            i += 1;
        } else {
            out.push(right);
            i += 1;
            j += 1;
        }
    }

    out
}

pub(crate) fn evaluate_query<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> QueryMatch {
    match query {
        ParsedQuery::Term(term) => leaf_query_match(tokens, term, MatchExtentKind::Term, false),
        ParsedQuery::Prefix(prefix) => {
            leaf_query_match(tokens, prefix, MatchExtentKind::Prefix, true)
        }
        ParsedQuery::Phrase(terms) => {
            let items: Vec<ParsedQuery> = terms.iter().cloned().map(ParsedQuery::Term).collect();
            range_matches_to_query(chain_ranges(tokens, &items, 1), MatchExtentKind::Phrase)
        }
        ParsedQuery::FollowedBy(items, distance) => range_matches_to_query(
            chain_ranges(tokens, items, *distance),
            MatchExtentKind::Proximity,
        ),
        ParsedQuery::And(items) => {
            let mut extents = Vec::new();
            for item in items {
                let child = evaluate_query(tokens, item);
                if !child.matched {
                    return QueryMatch::default();
                }
                extents.extend(child.extents);
            }
            QueryMatch {
                matched: true,
                extents,
            }
        }
        ParsedQuery::Or(items) => {
            let mut extents = Vec::new();
            let mut matched = false;
            for item in items {
                let child = evaluate_query(tokens, item);
                if child.matched {
                    matched = true;
                    extents.extend(child.extents);
                }
            }
            QueryMatch { matched, extents }
        }
        ParsedQuery::Not(item) => QueryMatch {
            matched: !evaluate_query(tokens, item).matched,
            extents: Vec::new(),
        },
    }
}

// `positive_ranges()` is intentionally highlight-oriented for `Or`: it returns the
// union of all matched positive branches. Scorers that need branch-level `Or = max`
// semantics must recurse on `Or` themselves instead of consuming these merged ranges.
pub(crate) fn positive_ranges<T: TokenLike>(tokens: &[T], query: &ParsedQuery) -> Vec<RangeMatch> {
    match query {
        ParsedQuery::Term(term) => collect_leaf_ranges(tokens, term, false),
        ParsedQuery::Prefix(prefix) => collect_leaf_ranges(tokens, prefix, true),
        ParsedQuery::Phrase(terms) => {
            let items: Vec<ParsedQuery> = terms.iter().cloned().map(ParsedQuery::Term).collect();
            chain_ranges(tokens, &items, 1)
        }
        ParsedQuery::FollowedBy(items, distance) => chain_ranges(tokens, items, *distance),
        ParsedQuery::And(items) => {
            if !matches_query(tokens, query) {
                return Vec::new();
            }
            let mut ranges = Vec::new();
            for item in items {
                if !matches!(item, ParsedQuery::Not(_)) {
                    ranges.extend(positive_ranges(tokens, item));
                }
            }
            sort_and_dedup_ranges(&mut ranges);
            ranges
        }
        ParsedQuery::Or(items) => {
            let mut ranges = Vec::new();
            for item in items {
                if matches_query(tokens, item) {
                    ranges.extend(positive_ranges(tokens, item));
                }
            }
            sort_and_dedup_ranges(&mut ranges);
            ranges
        }
        ParsedQuery::Not(_) => Vec::new(),
    }
}

fn leaf_query_match<T: TokenLike>(
    tokens: &[T],
    needle: &str,
    kind: MatchExtentKind,
    prefix: bool,
) -> QueryMatch {
    range_matches_to_query(collect_leaf_ranges(tokens, needle, prefix), kind)
}

fn range_matches_to_query(ranges: Vec<RangeMatch>, kind: MatchExtentKind) -> QueryMatch {
    if ranges.is_empty() {
        return QueryMatch::default();
    }
    QueryMatch {
        matched: true,
        extents: ranges
            .into_iter()
            .map(|range| MatchExtent {
                start_pos: range.start_pos,
                end_pos: range.end_pos,
                kind,
            })
            .collect(),
    }
}

fn collect_leaf_ranges<T: TokenLike>(tokens: &[T], needle: &str, prefix: bool) -> Vec<RangeMatch> {
    let mut ranges = Vec::new();
    for token in tokens {
        let matched = if prefix {
            token.term().starts_with(needle)
        } else {
            token.term() == needle
        };
        if matched {
            let pos = token.position();
            ranges.push(RangeMatch {
                start_pos: pos,
                end_pos: pos,
            });
        }
    }
    ranges
}

fn chain_ranges<T: TokenLike>(
    tokens: &[T],
    items: &[ParsedQuery],
    distance: u32,
) -> Vec<RangeMatch> {
    if distance == 0 || items.is_empty() {
        return Vec::new();
    }

    let mut ranges = positive_ranges(tokens, &items[0]);
    if ranges.is_empty() {
        return Vec::new();
    }

    for item in items.iter().skip(1) {
        let right = positive_ranges(tokens, item);
        if right.is_empty() {
            return Vec::new();
        }
        ranges = link_ranges_by_distance(&ranges, &right, distance);
        if ranges.is_empty() {
            return Vec::new();
        }
    }

    sort_and_dedup_ranges(&mut ranges);
    ranges
}

fn link_ranges_by_distance(
    left: &[RangeMatch],
    right: &[RangeMatch],
    distance: u32,
) -> Vec<RangeMatch> {
    let mut right_by_start: BTreeMap<TokenPosition, Vec<RangeMatch>> = BTreeMap::new();
    for range in right {
        right_by_start
            .entry(range.start_pos)
            .or_default()
            .push(*range);
    }

    let mut out = Vec::new();
    for left_range in left {
        let Some(target_start) = left_range.end_pos.checked_add(distance) else {
            continue;
        };
        let Some(right_ranges) = right_by_start.get(&target_start) else {
            continue;
        };
        for right_range in right_ranges {
            out.push(RangeMatch {
                start_pos: left_range.start_pos,
                end_pos: right_range.end_pos,
            });
        }
    }
    sort_and_dedup_ranges(&mut out);
    out
}

fn sort_and_dedup_ranges(ranges: &mut Vec<RangeMatch>) {
    ranges.sort_by_key(|range| (range.start_pos, range.end_pos));
    ranges.dedup();
}

fn accumulate_query_stats(query: &ParsedQuery, stats: &mut QueryStats) {
    stats.clause_count += 1;
    match query {
        ParsedQuery::Term(_) => {
            stats.positive_lexemes += 1;
        }
        ParsedQuery::Prefix(_) => {
            stats.positive_lexemes += 1;
            stats.has_prefix = true;
        }
        ParsedQuery::Phrase(terms) => {
            stats.positive_lexemes += terms.len();
            stats.has_phrase = true;
        }
        ParsedQuery::FollowedBy(items, _) => {
            stats.has_proximity = true;
            for item in items {
                accumulate_query_stats(item, stats);
            }
        }
        ParsedQuery::And(items) | ParsedQuery::Or(items) => {
            for item in items {
                accumulate_query_stats(item, stats);
            }
        }
        ParsedQuery::Not(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens_from_terms(terms: &[&str]) -> Vec<Token> {
        terms
            .iter()
            .enumerate()
            .map(|(idx, term)| Token::new((*term).to_string(), idx as TokenPosition))
            .collect()
    }

    #[test]
    fn term_and_prefix_match_collect_positions() {
        let tokens = tokens_from_terms(&["vector", "database", "vectors"]);
        let term_extents = collect_match_extents(&tokens, &ParsedQuery::Term("vector".to_string()));
        assert_eq!(term_extents.len(), 1);
        assert_eq!(term_extents[0].start_pos, 0);

        let prefix_extents =
            collect_match_extents(&tokens, &ParsedQuery::Prefix("vec".to_string()));
        assert_eq!(prefix_extents.len(), 2);
        assert_eq!(prefix_extents[1].start_pos, 2);
    }

    #[test]
    fn phrase_and_proximity_collect_ranges() {
        let tokens = tokens_from_terms(&["alpha", "beta", "gamma", "alpha", "x", "gamma"]);
        let phrase = ParsedQuery::Phrase(vec!["alpha".to_string(), "beta".to_string()]);
        let phrase_extents = collect_match_extents(&tokens, &phrase);
        assert_eq!(phrase_extents.len(), 1);
        assert_eq!(phrase_extents[0].start_pos, 0);
        assert_eq!(phrase_extents[0].end_pos, 1);

        let proximity = ParsedQuery::FollowedBy(
            vec![
                ParsedQuery::Term("alpha".to_string()),
                ParsedQuery::Term("gamma".to_string()),
            ],
            2,
        );
        let proximity_extents = collect_match_extents(&tokens, &proximity);
        assert_eq!(proximity_extents.len(), 2);
        assert_eq!(proximity_extents[0].start_pos, 0);
        assert_eq!(proximity_extents[0].end_pos, 2);
        assert_eq!(proximity_extents[1].start_pos, 3);
        assert_eq!(proximity_extents[1].end_pos, 5);
    }

    #[test]
    fn and_or_not_preserve_boolean_contract() {
        let tokens = tokens_from_terms(&["vector", "database", "spam"]);
        let query = ParsedQuery::And(vec![
            ParsedQuery::Term("vector".to_string()),
            ParsedQuery::Not(Box::new(ParsedQuery::Term("noise".to_string()))),
        ]);
        assert!(matches_query(&tokens, &query));
        let extents = collect_match_extents(&tokens, &query);
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].start_pos, 0);

        let or_query = ParsedQuery::Or(vec![
            ParsedQuery::Term("vector".to_string()),
            ParsedQuery::Term("database".to_string()),
        ]);
        let or_extents = collect_match_extents(&tokens, &or_query);
        assert_eq!(or_extents.len(), 2);

        let negative = ParsedQuery::And(vec![
            ParsedQuery::Term("vector".to_string()),
            ParsedQuery::Not(Box::new(ParsedQuery::Term("spam".to_string()))),
        ]);
        assert!(!matches_query(&tokens, &negative));
        assert!(collect_match_extents(&tokens, &negative).is_empty());
    }

    #[test]
    fn query_stats_tracks_positive_lexemes_and_features() {
        let query = ParsedQuery::And(vec![
            ParsedQuery::Prefix("vec".to_string()),
            ParsedQuery::FollowedBy(
                vec![
                    ParsedQuery::Term("alpha".to_string()),
                    ParsedQuery::Term("beta".to_string()),
                ],
                1,
            ),
        ]);
        let stats = query_stats(&query);
        assert_eq!(stats.positive_lexemes, 3);
        assert!(stats.has_prefix);
        assert!(stats.has_proximity);
    }
}
