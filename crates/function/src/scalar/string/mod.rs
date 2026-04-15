//! # String Functions
//!
//! String manipulation functions for Paro database.
//!
//!
//!
//! ## Implemented Functions
//! - `length`, `char_length`, `octet_length` - Length functions
//! - `lower`, `upper` - Case conversion
//! - `concat`, `concat_ws` - String concatenation
//! - `substring`, `left`, `right` - Substring extraction
//! - `trim`, `ltrim`, `rtrim` - Whitespace trimming
//! - `contains`, `position`, `instr` - String search
//! - `replace` - String replacement
//! - `prefix`, `suffix` - Prefix/suffix matching
//! - `regexp` (~), `not_regexp` (!~), `regexp_insensitive` (~*), `not_regexp_insensitive` (!~*) - Regex match

mod case_convert;
mod concat;
mod contains;
mod length;
mod prefix;
mod regexp;
mod replace;
mod substring;
mod trim;

pub use case_convert::*;
pub use concat::*;
pub use contains::*;
pub use length::*;
pub use prefix::*;
pub use regexp::*;
pub use replace::*;
pub use substring::*;
pub use trim::*;

use paro_common::error::Result;

use crate::{
    BoundScalarFunction, DictionaryStrategy, FunctionErrorMode, ScalarBindInput, ScalarFunction,
    ScalarFunctionSet,
};

pub(super) fn bind_storage_dictionary_unary_infallible(
    function: &ScalarFunction,
    _input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    Ok(BoundScalarFunction::from(function.clone())
        .with_error_mode(FunctionErrorMode::Infallible)
        .with_dictionary_strategy(DictionaryStrategy::StorageDictionaryCache { input_idx: 0 }))
}

/// Register all string functions.
pub fn register_string_functions() -> Vec<ScalarFunctionSet> {
    vec![
        // Length functions
        get_length_functions(),
        get_char_length_functions(),
        get_octet_length_functions(),
        // Case conversion
        get_lower_function(),
        get_upper_function(),
        // Concatenation
        get_concat_functions(),
        get_concat_ws_functions(),
        // Substring
        get_substring_functions(),
        get_left_functions(),
        get_right_functions(),
        // Trim
        get_trim_functions(),
        get_ltrim_functions(),
        get_rtrim_functions(),
        // Search
        get_contains_functions(),
        get_position_functions(),
        get_instr_functions(),
        // Replace
        get_replace_functions(),
        // Prefix/Suffix
        get_prefix_functions(),
        get_suffix_functions(),
        // Regexp
        get_regexp_functions(),
        get_regexp_insensitive_functions(),
        get_not_regexp_functions(),
        get_not_regexp_insensitive_functions(),
    ]
}
