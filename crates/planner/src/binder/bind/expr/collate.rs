//! Collate Expression Binding
//!
//!
//!
//! ## Supported Collations
//! - "C" / "POSIX": Binary comparison (default)
//! - "NOCASE": Case-insensitive comparison
//!
//! ## Known Limitations
//! - Only basic collations supported
//! - No ICU collation support

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::expression::Expression;

/// Supported collation names
const SUPPORTED_COLLATIONS: &[&str] = &["C", "POSIX", "NOCASE", "NOACCENT"];

/// Validates a collation name and returns the normalized form.
///
/// # Arguments
/// * `collation` - The collation name to validate
///
/// # Returns
/// * `Ok(String)` - The normalized collation name
/// * `Err` - If the collation is not supported
pub fn validate_collation(collation: &str) -> Result<String> {
    let normalized = collation.to_uppercase();

    if SUPPORTED_COLLATIONS.contains(&normalized.as_str()) {
        Ok(normalized)
    } else {
        Err(paro_error::syntax(format!(
            "Unsupported collation '{}'. Supported collations: {}",
            collation,
            SUPPORTED_COLLATIONS.join(", ")
        )))
    }
}

/// Binds a COLLATE expression.
///
/// the return type of the child expression to include collation information.
///
/// # Arguments
/// * `child` - The bound child expression
/// * `collation` - The collation name
///
/// # Returns
/// * `Ok(Expression)` - The child expression with modified return type
/// * `Err` - If the child is not VARCHAR or collation is invalid
pub fn bind_collate(mut child: Expression, collation: &str) -> Result<Expression> {
    // Validate that child is VARCHAR type
    let child_type = child.return_type();
    if !child_type.is_varchar() {
        return Err(paro_error::syntax(format!(
            "COLLATE is only supported for VARCHAR type, got {}",
            child_type
        )));
    }

    // Validate and normalize the collation name
    let normalized_collation = validate_collation(collation)?;

    // Create the new return type with collation
    let new_type = LogicalType::varchar_collation(normalized_collation);

    // Update the child expression's return type
    // We need to handle each expression variant that can have VARCHAR type
    update_expression_type(&mut child, new_type)?;

    Ok(child)
}

/// Updates the return type of an expression to include collation.
fn update_expression_type(expr: &mut Expression, new_type: LogicalType) -> Result<()> {
    match expr {
        Expression::Constant(ref mut c) => {
            c.return_type = new_type;
        }
        Expression::ColumnRef(ref mut c) => {
            c.return_type = new_type;
        }
        Expression::Function(ref mut f) => {
            f.return_type = new_type;
        }
        Expression::Cast(ref mut c) => {
            c.target_type = new_type;
        }
        Expression::Operator(ref mut o) => {
            o.return_type = new_type;
        }
        Expression::Reference(ref mut r) => {
            r.return_type = new_type;
        }
        Expression::Case(ref mut c) => {
            c.return_type = new_type;
        }
        _ => {
            return Err(paro_error::syntax(
                "Cannot apply COLLATE to this expression type".to_string(),
            ));
        }
    }
    Ok(())
}
