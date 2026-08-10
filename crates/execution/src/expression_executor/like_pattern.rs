// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compiled SQL `LIKE` patterns and selection kernels.

use memchr::memmem::Finder;
use paro_common::error::Result;
use paro_common::vector::{SelectionVector, Vector};

use super::predicate::map_row;

/// Allocation-free matcher for a constant ASCII `LIKE` pattern without `_`.
///
/// Dynamic patterns and patterns containing the single-character wildcard use
/// [`sql_like`]. Keeping the prepared form exact but deliberately narrow makes
/// the analytical prefix/substring path cheap without duplicating the full
/// Unicode matcher.
#[derive(Debug)]
pub(crate) struct PreparedLikePattern {
    original: String,
    case_insensitive: bool,
    strategy: LikeStrategy,
}

#[derive(Debug)]
enum LikeStrategy {
    Any,
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Suffix(Vec<u8>),
    Contains(LiteralSearcher),
    Ordered {
        segments: Vec<LiteralSearcher>,
        anchored_start: bool,
        anchored_end: bool,
    },
}

#[derive(Debug)]
struct LiteralSearcher {
    literal: Vec<u8>,
    finder: Option<Finder<'static>>,
    shifts: Option<Box<[usize; 256]>>,
}

impl LiteralSearcher {
    const SKIP_SEARCH_MIN_LENGTH: usize = 4;

    fn new(literal: Vec<u8>, case_insensitive: bool) -> Self {
        let finder = (!case_insensitive).then(|| Finder::new(&literal).into_owned());
        let shifts =
            (case_insensitive && literal.len() >= Self::SKIP_SEARCH_MIN_LENGTH).then(|| {
                let mut shifts = Box::new([literal.len(); 256]);
                for (idx, &byte) in literal[..literal.len() - 1].iter().enumerate() {
                    shifts[normalize(byte, case_insensitive) as usize] = literal.len() - idx - 1;
                }
                shifts
            });
        Self {
            literal,
            finder,
            shifts,
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.literal
    }

    fn find_in(&self, haystack: &[u8], case_insensitive: bool) -> Option<usize> {
        if let Some(finder) = &self.finder {
            debug_assert!(!case_insensitive);
            return finder.find(haystack);
        }
        let needle = self.as_bytes();
        if needle.is_empty() {
            return Some(0);
        }
        let Some(shifts) = &self.shifts else {
            return find_bytes_linear(haystack, needle, case_insensitive);
        };
        let mut end = needle.len().checked_sub(1)?;
        while end < haystack.len() {
            let start = end + 1 - needle.len();
            if bytes_equal(&haystack[start..=end], needle, case_insensitive) {
                return Some(start);
            }
            end = end.checked_add(shifts[normalize(haystack[end], case_insensitive) as usize])?;
        }
        None
    }
}

impl PreparedLikePattern {
    pub(crate) fn try_new(pattern: &str, case_insensitive: bool) -> Option<Self> {
        if !pattern.is_ascii() {
            return None;
        }

        let bytes = pattern.as_bytes();
        let mut segments = Vec::new();
        let mut current = Vec::new();
        let mut has_any = false;
        let mut first_token_seen = false;
        let mut anchored_start = true;
        let mut ends_with_any = false;
        let mut idx = 0usize;
        while idx < bytes.len() {
            match bytes[idx] {
                b'\\' if idx + 1 < bytes.len() => {
                    current.push(normalize(bytes[idx + 1], case_insensitive));
                    first_token_seen = true;
                    ends_with_any = false;
                    idx += 2;
                }
                b'%' => {
                    if !first_token_seen {
                        anchored_start = false;
                    }
                    first_token_seen = true;
                    has_any = true;
                    ends_with_any = true;
                    if !current.is_empty() {
                        segments.push(std::mem::take(&mut current));
                    }
                    idx += 1;
                }
                b'_' => return None,
                byte => {
                    current.push(normalize(byte, case_insensitive));
                    first_token_seen = true;
                    ends_with_any = false;
                    idx += 1;
                }
            }
        }
        if !current.is_empty() || !has_any {
            segments.push(current);
        }
        let anchored_end = !ends_with_any;

        let strategy = if !has_any {
            LikeStrategy::Exact(segments.pop().unwrap_or_default())
        } else if segments.is_empty() {
            LikeStrategy::Any
        } else if segments.len() == 1 {
            let literal = segments.pop().expect("single LIKE segment");
            match (anchored_start, anchored_end) {
                (true, true) => LikeStrategy::Exact(literal),
                (true, false) => LikeStrategy::Prefix(literal),
                (false, true) => LikeStrategy::Suffix(literal),
                (false, false) => {
                    LikeStrategy::Contains(LiteralSearcher::new(literal, case_insensitive))
                }
            }
        } else {
            LikeStrategy::Ordered {
                segments: segments
                    .into_iter()
                    .map(|segment| LiteralSearcher::new(segment, case_insensitive))
                    .collect(),
                anchored_start,
                anchored_end,
            }
        };
        Some(Self {
            original: pattern.to_owned(),
            case_insensitive,
            strategy,
        })
    }

    #[inline]
    pub(crate) fn matches(&self, value: &str) -> bool {
        if self.case_insensitive && !value.is_ascii() {
            return sql_like(value, &self.original, true);
        }
        let value = value.as_bytes();
        match &self.strategy {
            LikeStrategy::Any => true,
            LikeStrategy::Exact(literal) => bytes_equal(value, literal, self.case_insensitive),
            LikeStrategy::Prefix(literal) => value
                .get(..literal.len())
                .is_some_and(|prefix| bytes_equal(prefix, literal, self.case_insensitive)),
            LikeStrategy::Suffix(literal) => value
                .len()
                .checked_sub(literal.len())
                .and_then(|start| value.get(start..))
                .is_some_and(|suffix| bytes_equal(suffix, literal, self.case_insensitive)),
            LikeStrategy::Contains(literal) => {
                literal.find_in(value, self.case_insensitive).is_some()
            }
            LikeStrategy::Ordered {
                segments,
                anchored_start,
                anchored_end,
            } => matches_ordered_segments(
                value,
                segments,
                *anchored_start,
                *anchored_end,
                self.case_insensitive,
            ),
        }
    }
}

pub(crate) fn select_prepared_like(
    values: &Vector,
    pattern: &PreparedLikePattern,
    input_sel: Option<&SelectionVector>,
    count: usize,
    output: &mut SelectionVector,
) -> Result<usize> {
    let values = values.try_to_varlen_view(count)?;
    output.set_len(count);
    let mut selected = 0usize;
    for row_idx in 0..count {
        if values.is_valid(row_idx) && pattern.matches(values.get_inline_string(row_idx).as_str()) {
            output.set(selected, map_row(input_sel, row_idx));
            selected += 1;
        }
    }
    output.set_len(selected);
    Ok(selected)
}

fn matches_ordered_segments(
    value: &[u8],
    segments: &[LiteralSearcher],
    anchored_start: bool,
    anchored_end: bool,
    case_insensitive: bool,
) -> bool {
    let mut cursor = 0usize;
    let mut segment_idx = 0usize;
    if anchored_start {
        let first = &segments[0];
        let Some(prefix) = value.get(..first.as_bytes().len()) else {
            return false;
        };
        if !bytes_equal(prefix, first.as_bytes(), case_insensitive) {
            return false;
        }
        cursor = first.as_bytes().len();
        segment_idx = 1;
    }

    let middle_end = segments.len() - usize::from(anchored_end);
    while segment_idx < middle_end {
        let segment = &segments[segment_idx];
        let Some(relative) = segment.find_in(&value[cursor..], case_insensitive) else {
            return false;
        };
        cursor += relative + segment.as_bytes().len();
        segment_idx += 1;
    }

    if anchored_end {
        let last = segments
            .last()
            .expect("ordered LIKE segments are non-empty");
        let Some(start) = value.len().checked_sub(last.as_bytes().len()) else {
            return false;
        };
        start >= cursor && bytes_equal(&value[start..], last.as_bytes(), case_insensitive)
    } else {
        true
    }
}

#[inline]
fn normalize(byte: u8, case_insensitive: bool) -> u8 {
    if case_insensitive {
        byte.to_ascii_lowercase()
    } else {
        byte
    }
}

#[inline]
fn bytes_equal(left: &[u8], right: &[u8], case_insensitive: bool) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(&left, &right)| {
            normalize(left, case_insensitive) == normalize(right, case_insensitive)
        })
}

fn find_bytes_linear(haystack: &[u8], needle: &[u8], case_insensitive: bool) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| bytes_equal(window, needle, case_insensitive))
}

pub(crate) fn sql_like(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    // TPC-H strings and patterns are ASCII. Keep that path allocation-free;
    // the Unicode path below only allocates the two linear character arrays.
    if value.is_ascii() && pattern.is_ascii() {
        return sql_like_tokens(
            value.as_bytes(),
            pattern.as_bytes(),
            b'%',
            b'_',
            b'\\',
            |left, right| {
                if case_insensitive {
                    left.eq_ignore_ascii_case(&right)
                } else {
                    left == right
                }
            },
        );
    }

    // Unicode case folding can change the number of scalar values, so normalize
    // both strings before tokenization instead of folding individual characters.
    let (value, pattern) = if case_insensitive {
        (value.to_lowercase(), pattern.to_lowercase())
    } else {
        (value.to_owned(), pattern.to_owned())
    };
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    sql_like_tokens(&value, &pattern, '%', '_', '\\', |left, right| {
        left == right
    })
}

fn sql_like_tokens<T, F>(value: &[T], pattern: &[T], any: T, one: T, escape: T, equals: F) -> bool
where
    T: Copy + PartialEq,
    F: Fn(T, T) -> bool,
{
    let mut value_idx = 0;
    let mut pattern_idx = 0;
    let mut wildcard = None;
    let mut wildcard_value_idx = 0;

    while value_idx < value.len() {
        if pattern_idx < pattern.len() {
            let token = pattern[pattern_idx];
            if token == any {
                wildcard = Some(pattern_idx);
                pattern_idx += 1;
                wildcard_value_idx = value_idx;
                continue;
            }
            if token == one {
                value_idx += 1;
                pattern_idx += 1;
                continue;
            }
            if token == escape && pattern_idx + 1 < pattern.len() {
                if equals(value[value_idx], pattern[pattern_idx + 1]) {
                    value_idx += 1;
                    pattern_idx += 2;
                    continue;
                }
            } else if equals(value[value_idx], token) {
                value_idx += 1;
                pattern_idx += 1;
                continue;
            }
        }

        let Some(wildcard_idx) = wildcard else {
            return false;
        };
        wildcard_value_idx += 1;
        value_idx = wildcard_value_idx;
        pattern_idx = wildcard_idx + 1;
    }

    while pattern_idx < pattern.len() && pattern[pattern_idx] == any {
        pattern_idx += 1;
    }
    pattern_idx == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_like_matches_sql_wildcards_and_escapes() {
        assert!(sql_like("PROMO BURNISHED COPPER", "PROMO%", false));
        assert!(sql_like(
            "special instructions and requests",
            "%special%requests%",
            false
        ));
        assert!(sql_like("A_B", "A\\_B", false));
        assert!(sql_like("aXb", "A_B", true));
        assert!(!sql_like("BRASS PLATED", "%BRASS", false));
        assert!(sql_like("100%", "100\\%", false));
        assert!(!sql_like("100\\%", "100\\%", false));
        assert!(sql_like("你好世界", "你_世%", false));
        assert!(sql_like("Éclair", "é%", true));
        assert!(!sql_like("anything", "", false));
        assert!(sql_like("", "", false));
        assert!(sql_like("", "%", false));
    }

    #[test]
    fn prepared_like_matches_constant_pattern_shapes() {
        for (value, pattern, case_insensitive, expected) in [
            ("PROMO BURNISHED", "PROMO%", false, true),
            ("BURNISHED PROMO", "%PROMO", false, true),
            (
                "A Customer with Complaints here",
                "%Customer%Complaints%",
                false,
                true,
            ),
            (
                "Complaints before Customer",
                "%Customer%Complaints%",
                false,
                false,
            ),
            ("100%", "100\\%", false, true),
            ("brand#45", "BRAND#45", true, true),
            ("", "", false, true),
        ] {
            let prepared = PreparedLikePattern::try_new(pattern, case_insensitive)
                .expect("pattern should be preparable");
            assert_eq!(
                prepared.matches(value),
                expected,
                "value={value:?}, pattern={pattern:?}"
            );
            assert_eq!(
                prepared.matches(value),
                sql_like(value, pattern, case_insensitive)
            );
        }
        assert!(PreparedLikePattern::try_new("A_B", false).is_none());
        assert!(PreparedLikePattern::try_new("é%", true).is_none());
    }

    #[test]
    fn literal_searcher_matches_linear_search() {
        for (haystack, needle, case_insensitive) in [
            ("prefix Customer suffix", "Customer", false),
            ("prefix customer suffix", "CUSTOMER", true),
            ("aaaaaaaaaaaaaaaaab", "aaaab", false),
            ("short", "longer needle", false),
            ("embedded needle twice needle", "needle", false),
        ] {
            let searcher = LiteralSearcher::new(
                needle
                    .bytes()
                    .map(|byte| normalize(byte, case_insensitive))
                    .collect(),
                case_insensitive,
            );
            assert_eq!(
                searcher.find_in(haystack.as_bytes(), case_insensitive),
                find_bytes_linear(haystack.as_bytes(), searcher.as_bytes(), case_insensitive),
                "haystack={haystack:?}, needle={needle:?}"
            );
        }
    }

    #[test]
    fn generic_like_handles_long_wildcard_patterns_in_linear_space() {
        let value = "a".repeat(8_192);
        let pattern = format!("%{}b", "a%".repeat(4_096));
        assert!(!sql_like(&value, &pattern, false));
    }
}
