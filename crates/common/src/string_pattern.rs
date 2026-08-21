// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compiled SQL string-pattern semantics shared by execution and storage.

use memchr::memmem::Finder;

/// Allocation-free matcher for a constant ASCII `LIKE` pattern without `_`.
///
/// Dynamic patterns and patterns containing the single-character wildcard use
/// [`sql_like`]. The prepared form deliberately has one implementation so scan
/// pushdown and residual expression evaluation cannot drift semantically.
#[derive(Debug)]
pub struct PreparedLikePattern {
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
    Contains(Box<LiteralSearcher>),
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

/// A literal that every match of a prepared `LIKE` pattern must contain.
///
/// Storage kernels use this opaque view to search a contiguous page once and
/// only run the complete matcher for rows containing the anchor. The anchor is
/// a necessary condition, never a sufficient one: callers must still verify a
/// candidate with [`PreparedLikePattern::matches_bytes`].
#[derive(Clone, Copy)]
pub struct PreparedLikeSearchAnchor<'a> {
    searcher: &'a LiteralSearcher,
    case_insensitive: bool,
}

impl PreparedLikeSearchAnchor<'_> {
    #[inline]
    pub fn literal(&self) -> &[u8] {
        self.searcher.as_bytes()
    }

    #[inline]
    pub fn find_in(&self, haystack: &[u8]) -> Option<usize> {
        self.searcher.find_in(haystack, self.case_insensitive)
    }
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
    /// Compile a constant ASCII pattern whose only wildcard is `%`.
    pub fn try_new(pattern: &str, case_insensitive: bool) -> Option<Self> {
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
                (false, false) => LikeStrategy::Contains(Box::new(LiteralSearcher::new(
                    literal,
                    case_insensitive,
                ))),
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
    pub fn matches(&self, value: &str) -> bool {
        if self.case_insensitive && !value.is_ascii() {
            return sql_like(value, &self.original, true);
        }
        self.matches_bytes(value.as_bytes())
    }

    /// Match bytes directly. Case-sensitive prepared patterns are exact for
    /// arbitrary UTF-8 values; case-insensitive callers must provide ASCII.
    #[inline]
    pub fn matches_bytes(&self, value: &[u8]) -> bool {
        debug_assert!(!self.case_insensitive || value.is_ascii());
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

    /// Return the longest literal segment that is present in every match.
    ///
    /// Exact/prefix/suffix patterns already have cheaper row kernels. Contains
    /// and ordered patterns benefit from using this literal as a page-level
    /// candidate generator. Choosing the longest segment minimizes false
    /// positives without changing SQL semantics.
    pub fn search_anchor(&self) -> Option<PreparedLikeSearchAnchor<'_>> {
        // Case-insensitive `matches(&str)` follows Unicode lowercase rules for
        // non-ASCII values. A normalized ASCII pattern literal is therefore
        // not a necessary byte substring (for example `k` matches `K`). Only
        // an explicit ASCII-domain proof could make such an anchor sound.
        if self.case_insensitive {
            return None;
        }
        let searcher = match &self.strategy {
            LikeStrategy::Contains(searcher) => searcher,
            LikeStrategy::Ordered { segments, .. } => segments
                .iter()
                .filter(|segment| !segment.as_bytes().is_empty())
                .max_by_key(|segment| segment.as_bytes().len())?,
            LikeStrategy::Any
            | LikeStrategy::Exact(_)
            | LikeStrategy::Prefix(_)
            | LikeStrategy::Suffix(_) => return None,
        };
        Some(PreparedLikeSearchAnchor {
            searcher,
            case_insensitive: self.case_insensitive,
        })
    }
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

pub fn sql_like(value: &str, pattern: &str, case_insensitive: bool) -> bool {
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
            assert_eq!(prepared.matches(value), expected);
            assert_eq!(
                prepared.matches(value),
                sql_like(value, pattern, case_insensitive)
            );
        }
        assert!(PreparedLikePattern::try_new("A_B", false).is_none());
        assert!(PreparedLikePattern::try_new("é%", true).is_none());
    }

    #[test]
    fn byte_matching_agrees_for_non_ascii_values_and_ascii_patterns() {
        for (value, pattern) in [("你好绿色世界", "%green%"), ("你好BRASS", "%BRASS")] {
            let prepared = PreparedLikePattern::try_new(pattern, false).unwrap();
            assert_eq!(
                prepared.matches_bytes(value.as_bytes()),
                prepared.matches(value)
            );
        }
    }

    #[test]
    fn prepared_like_exposes_the_longest_necessary_search_anchor() {
        let prepared = PreparedLikePattern::try_new("%special%long requests%", false).unwrap();
        let anchor = prepared.search_anchor().expect("ordered LIKE anchor");

        assert_eq!(anchor.literal(), b"long requests");
        assert_eq!(anchor.find_in(b"prefix long requests suffix"), Some(7));
        assert!(prepared.matches_bytes(b"special long requests"));
        assert!(!prepared.matches_bytes(b"long requests before special"));

        let prefix = PreparedLikePattern::try_new("PROMO%", false).unwrap();
        assert!(prefix.search_anchor().is_none());

        let case_insensitive = PreparedLikePattern::try_new("%k%", true).unwrap();
        assert!(case_insensitive.matches("K"));
        assert!(case_insensitive.search_anchor().is_none());
    }

    #[test]
    fn generic_like_handles_long_wildcard_patterns_in_linear_space() {
        let value = "a".repeat(8_192);
        let pattern = format!("%{}b", "a%".repeat(4_096));
        assert!(!sql_like(&value, &pattern, false));
    }
}
