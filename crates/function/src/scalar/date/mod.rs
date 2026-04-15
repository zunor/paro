//! # Date/Time Functions
//!
//! Date and time manipulation functions for Paro database.
//!
//!
//!
//! ## Implemented Functions
//! - `extract`, `date_part` - Extract date/time components
//! - `year`, `month`, `day`, `hour`, `minute`, `second` - Shorthand extractors
//! - `now`, `current_date`, `current_time`, `current_timestamp` - Current time
//! - `date_add`, `date_sub` - Date arithmetic
//! - `date_diff`, `datediff` - Date difference
//! - `age` - Interval between dates
//! - `epoch` - Convert to/from Unix epoch

mod current;
mod date_arithmetic;
mod date_part;
mod epoch;

pub use current::*;
pub use date_arithmetic::*;
pub use date_part::*;
pub use epoch::*;

use crate::ScalarFunctionSet;

/// Register all date/time functions.
pub fn register_date_functions() -> Vec<ScalarFunctionSet> {
    vec![
        // Current time functions
        get_now_function(),
        get_current_date_function(),
        get_current_time_function(),
        get_current_timestamp_function(),
        // Date part extraction
        get_extract_functions(),
        get_date_part_functions(),
        get_year_functions(),
        get_month_functions(),
        get_day_functions(),
        get_hour_functions(),
        get_minute_functions(),
        get_second_functions(),
        get_dayofweek_functions(),
        get_dayofyear_functions(),
        get_week_functions(),
        get_quarter_functions(),
        // Date arithmetic
        get_date_add_functions(),
        get_date_sub_functions(),
        get_date_diff_functions(),
        get_age_functions(),
        // Epoch conversion
        get_epoch_functions(),
        get_epoch_ms_functions(),
        get_to_timestamp_functions(),
    ]
}
