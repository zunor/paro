//! # Math Functions
//!
//! Mathematical functions for Paro database.
//!
//!
//!
//! ## Implemented Functions
//! - `abs` - Absolute value
//! - `ceil`, `ceiling` - Round up to nearest integer
//! - `floor` - Round down to nearest integer
//! - `round` - Round to nearest integer or decimal places
//! - `trunc`, `truncate` - Truncate toward zero
//! - `exp` - Exponential (e^x)
//! - `ln` - Natural logarithm
//! - `log`, `log10` - Base-10 logarithm
//! - `log2` - Base-2 logarithm
//! - `pow`, `power` - Exponentiation
//! - `sqrt` - Square root
//! - `cbrt` - Cube root
//! - `sin`, `cos`, `tan` - Trigonometric functions
//! - `asin`, `acos`, `atan`, `atan2` - Inverse trigonometric functions
//! - `sign` - Sign of a number (-1, 0, 1)
//! - `pi` - Mathematical constant π
//! - `random` - Random number (volatile)
//! - `greatest`, `least` - Multi-value min/max

mod logarithm;
mod numeric;
mod special;
mod trigonometric;

pub use logarithm::*;
pub use numeric::*;
pub use special::*;
pub use trigonometric::*;

use crate::ScalarFunctionSet;

/// Register all math functions.
pub fn register_math_functions() -> Vec<ScalarFunctionSet> {
    vec![
        // Basic numeric functions
        get_abs_functions(),
        get_ceil_functions(),
        get_floor_functions(),
        get_round_functions(),
        get_trunc_functions(),
        get_sign_functions(),
        // Power and root functions
        get_exp_functions(),
        get_pow_functions(),
        get_sqrt_functions(),
        get_cbrt_functions(),
        // Logarithm functions
        get_ln_functions(),
        get_log_functions(),
        get_log2_functions(),
        get_log10_functions(),
        // Trigonometric functions
        get_sin_functions(),
        get_cos_functions(),
        get_tan_functions(),
        get_asin_functions(),
        get_acos_functions(),
        get_atan_functions(),
        get_atan2_functions(),
        // Special functions
        get_pi_function(),
        get_random_function(),
        get_greatest_functions(),
        get_least_functions(),
    ]
}
