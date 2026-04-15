use paro_common::error::Result;
use paro_storage::index::fulltext::query_parser::{parse_query, parse_to_tsquery, ParsedQuery};
use paro_storage::index::fulltext::tokenizer::{Token, Tokenizer};

const SIMPLE_CONFIG: &str = "simple";
const MIN_TOKEN_LEN: usize = 1;
const MAX_TOKEN_LEN: Option<usize> = None;

struct IdentityTokenizer;

impl Tokenizer for IdentityTokenizer {
    fn tokenize(&self, text: &str, out: &mut Vec<Token>) {
        if text.is_empty() {
            return;
        }
        out.push(Token::new(text.to_lowercase(), 0));
    }
}

pub(crate) fn parse_legacy_query(query: &str) -> Result<ParsedQuery> {
    let (_, tokenizer) =
        paro_storage::index::fulltext::tokenizer::tokenizer_from_config(SIMPLE_CONFIG)?;
    parse_query(query, tokenizer.as_ref(), MIN_TOKEN_LEN, MAX_TOKEN_LEN)
}

pub(crate) fn parse_internal_query(query: &str) -> Result<ParsedQuery> {
    let tokenizer = IdentityTokenizer;
    parse_to_tsquery(query, &tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
}

pub(crate) fn query_matches_text(query: &ParsedQuery, text: &str) -> bool {
    let tokens = tokenize_text(text);
    matches_query(query, &tokens)
}

pub(crate) fn score_query_text(query: &ParsedQuery, text: &str) -> f32 {
    let tokens = tokenize_text(text);
    score_query_recursive(query, &tokens)
}

fn tokenize_text(text: &str) -> Vec<String> {
    text.split_whitespace()
        .filter(|token| !token.is_empty())
        .map(|token| token.to_lowercase())
        .collect()
}

fn matches_query(query: &ParsedQuery, tokens: &[String]) -> bool {
    match query {
        ParsedQuery::Term(term) => tokens.iter().any(|token| token == term),
        ParsedQuery::Prefix(prefix) => tokens.iter().any(|token| token.starts_with(prefix)),
        ParsedQuery::Phrase(terms) => matches_followed_by_terms(tokens, terms, 1),
        ParsedQuery::FollowedBy(items, distance) => {
            matches_followed_by_query_items(tokens, items, *distance)
        }
        ParsedQuery::And(items) => items.iter().all(|item| matches_query(item, tokens)),
        ParsedQuery::Or(items) => items.iter().any(|item| matches_query(item, tokens)),
        ParsedQuery::Not(item) => !matches_query(item, tokens),
    }
}

fn matches_followed_by_query_items(
    tokens: &[String],
    items: &[ParsedQuery],
    distance: u32,
) -> bool {
    if distance == 0 || items.is_empty() {
        return false;
    }

    let mut reachable = match query_positions(&items[0], tokens) {
        Some(positions) => positions,
        None => return false,
    };
    for item in items.iter().skip(1) {
        let Some(next_positions) = query_positions(item, tokens) else {
            return false;
        };
        reachable = positions_following_by_distance(&reachable, &next_positions, distance);
        if reachable.is_empty() {
            return false;
        }
    }
    !reachable.is_empty()
}

fn matches_followed_by_terms(tokens: &[String], terms: &[String], distance: u32) -> bool {
    if distance == 0 || terms.is_empty() {
        return false;
    }
    let mut positions = match term_positions(&terms[0], tokens) {
        Some(positions) => positions,
        None => return false,
    };
    for term in terms.iter().skip(1) {
        let Some(next_positions) = term_positions(term, tokens) else {
            return false;
        };
        positions = positions_following_by_distance(&positions, &next_positions, distance);
        if positions.is_empty() {
            return false;
        }
    }
    !positions.is_empty()
}

fn query_positions(query: &ParsedQuery, tokens: &[String]) -> Option<Vec<u32>> {
    match query {
        ParsedQuery::Term(term) => term_positions(term, tokens),
        ParsedQuery::Prefix(prefix) => prefix_positions(prefix, tokens),
        ParsedQuery::Phrase(terms) => {
            let terms = terms.iter().map(String::as_str).collect::<Vec<_>>();
            term_chain_positions(&terms, tokens, 1)
        }
        ParsedQuery::FollowedBy(items, distance) => {
            if items
                .iter()
                .all(|item| matches!(item, ParsedQuery::Term(_) | ParsedQuery::Prefix(_)))
            {
                let mut term_strings = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        ParsedQuery::Term(term) => term_strings.push(term.as_str()),
                        ParsedQuery::Prefix(prefix) => {
                            return prefix_positions(prefix, tokens);
                        }
                        _ => return None,
                    }
                }
                term_chain_positions(&term_strings, tokens, *distance)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn term_chain_positions(terms: &[&str], tokens: &[String], distance: u32) -> Option<Vec<u32>> {
    if distance == 0 || terms.is_empty() {
        return None;
    }

    let mut positions = term_positions(terms[0], tokens)?;
    for term in terms.iter().skip(1) {
        let next_positions = term_positions(term, tokens)?;
        positions = positions_following_by_distance(&positions, &next_positions, distance);
        if positions.is_empty() {
            return None;
        }
    }
    Some(positions)
}

fn term_positions(term: &str, tokens: &[String]) -> Option<Vec<u32>> {
    let mut positions = Vec::new();
    for (idx, token) in tokens.iter().enumerate() {
        if token == term {
            positions.push(idx as u32);
        }
    }
    if positions.is_empty() {
        None
    } else {
        Some(positions)
    }
}

fn prefix_positions(prefix: &str, tokens: &[String]) -> Option<Vec<u32>> {
    let mut positions = Vec::new();
    for (idx, token) in tokens.iter().enumerate() {
        if token.starts_with(prefix) {
            positions.push(idx as u32);
        }
    }
    if positions.is_empty() {
        None
    } else {
        Some(positions)
    }
}

fn positions_following_by_distance(
    left_positions: &[u32],
    right_positions: &[u32],
    distance: u32,
) -> Vec<u32> {
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

fn score_query_recursive(query: &ParsedQuery, tokens: &[String]) -> f32 {
    match query {
        ParsedQuery::Term(term) => tokens.iter().any(|token| token == term) as i32 as f32,
        ParsedQuery::Prefix(prefix) => {
            tokens.iter().any(|token| token.starts_with(prefix)) as i32 as f32
        }
        ParsedQuery::Phrase(terms) => terms
            .iter()
            .map(|term| tokens.iter().any(|token| token == term) as i32 as f32)
            .sum(),
        ParsedQuery::FollowedBy(items, _) | ParsedQuery::And(items) | ParsedQuery::Or(items) => {
            items
                .iter()
                .map(|item| score_query_recursive(item, tokens))
                .sum()
        }
        ParsedQuery::Not(item) => score_query_recursive(item, tokens),
    }
}
