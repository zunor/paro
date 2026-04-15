use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::Expr;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct AliasLookup {
    alias_map: HashMap<String, usize>,
    original_expressions: Vec<Expr>,
    volatile_expressions: HashSet<usize>,
    subquery_expressions: HashSet<usize>,
}

impl AliasLookup {
    pub fn snapshot(state: &SelectBindState) -> Self {
        Self {
            alias_map: state.alias_map.clone(),
            original_expressions: state.original_expressions.clone(),
            volatile_expressions: state.volatile_expressions.clone(),
            subquery_expressions: state.subquery_expressions.clone(),
        }
    }

    pub fn get_alias_index(&self, name: &str) -> Option<usize> {
        self.alias_map
            .get(name)
            .copied()
            .or_else(|| self.alias_map.get(&name.to_lowercase()).copied())
    }

    pub fn resolve_alias(&self, name: &str) -> Result<Option<(usize, Expr)>> {
        let index = match self.get_alias_index(name) {
            Some(index) => index,
            None => return Ok(None),
        };
        if self.volatile_expressions.contains(&index) {
            return Err(paro_error::syntax(format!(
                "Alias \"{}\" referenced - but the expression has side effects. This is not yet supported.",
                name
            )));
        }
        if self.subquery_expressions.contains(&index) {
            return Err(paro_error::syntax(format!(
                "Alias \"{}\" referenced - but the expression has a subquery. This is not yet supported.",
                name
            )));
        }
        Ok(self
            .original_expressions
            .get(index)
            .cloned()
            .map(|expr| (index, expr)))
    }
}

/// State maintained during binding of a SELECT clause.
///
///
/// This structure tracks:
/// - Alias mappings for resolving column aliases in ORDER BY, HAVING, etc.
/// - Projection mappings for deduplicating expressions
/// - Volatile expression tracking to prevent referencing expressions with side effects
/// - Subquery expression tracking
/// - Expanded column indices for UNNEST operations
#[derive(Debug, Default, Clone)]
pub struct SelectBindState {
    /// Map from alias (case-insensitive) to SELECT list index.
    ///
    pub alias_map: HashMap<String, usize>,

    /// Map from expression string to projection index.
    /// Used to deduplicate expressions in the SELECT list.
    ///
    pub projection_map: HashMap<String, usize>,

    /// The original unparsed expressions. This is exported after binding,
    /// because the binding might change the expressions (e.g. when a * clause is present).
    ///
    pub original_expressions: Vec<Expr>,

    /// Whether the SELECT list has any volatile expressions.
    pub has_volatile: bool,

    /// Whether the SELECT list has any subqueries.
    pub has_subquery: bool,

    /// Whether the SELECT list has any window functions.
    pub has_window: bool,

    /// Whether the SELECT list has any aggregate functions.
    pub has_aggregate: bool,

    /// The set of referenced aliases.
    ///
    referenced_aliases: HashSet<usize>,

    /// The set of expressions that is volatile (has side effects).
    ///
    volatile_expressions: HashSet<usize>,

    /// The set of expressions that contains a subquery.
    ///
    subquery_expressions: HashSet<usize>,

    /// Column indices after expansion of Expanded expressions (e.g. UNNEST(STRUCT) clauses).
    /// This maps original SELECT list indices to final projection indices after expansion.
    ///
    expanded_column_indices: Vec<usize>,
}

impl SelectBindState {
    /// Create a new SelectBindState.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind an alias by index, returning a copy of the original expression.
    ///
    ///
    /// This method:
    /// 1. Checks if the expression is volatile (has side effects) - if so, returns an error
    /// 2. Marks the alias as referenced (for later volatile checking)
    /// 3. Returns a copy of the original expression
    ///
    /// # Errors
    /// Returns an error if the alias references a volatile expression.
    pub fn bind_alias(&mut self, index: usize) -> Result<Expr> {
        // Check if this expression is volatile
        if self.volatile_expressions.contains(&index) {
            let alias = self.get_expression_display_name(index);
            return Err(paro_error::syntax(format!(
                "Alias \"{}\" referenced - but the expression has side effects. \
                 This is not yet supported.",
                alias
            )));
        }

        // Mark this alias as referenced
        self.referenced_aliases.insert(index);

        // Return a copy of the original expression
        self.original_expressions
            .get(index)
            .cloned()
            .ok_or_else(|| paro_error::syntax(format!("Alias index {} out of bounds", index)))
    }

    /// Mark an alias as referenced.
    pub fn mark_alias_referenced(&mut self, index: usize) {
        self.referenced_aliases.insert(index);
    }

    /// Resolve a SELECT-list alias using the live binding state.
    pub fn resolve_select_alias(
        &mut self,
        alias: &str,
        current_index: Option<usize>,
    ) -> Result<Option<Expr>> {
        let index = match self.get_alias_index(alias) {
            Some(index) => index,
            None => return Ok(None),
        };
        if current_index == Some(index) {
            return Err(paro_error::syntax(format!(
                "Circular reference to alias \"{}\"",
                alias
            )));
        }
        if self.alias_has_subquery(index) {
            return Err(paro_error::syntax(format!(
                "Alias \"{}\" referenced in a SELECT clause - but the expression has a subquery. This is not yet supported.",
                alias
            )));
        }

        self.bind_alias(index).map(Some)
    }

    /// Mark an expression as volatile (has side effects).
    ///
    ///
    /// This method checks if the expression has already been referenced.
    /// If so, it returns an error because we cannot reference a volatile expression.
    ///
    /// # Errors
    /// Returns an error if the expression has already been referenced as an alias.
    pub fn set_expression_is_volatile(&mut self, index: usize) -> Result<()> {
        // Check if this expression has been referenced before
        if self.referenced_aliases.contains(&index) {
            let alias = self.get_expression_display_name(index);
            return Err(paro_error::syntax(format!(
                "Alias \"{}\" referenced - but the expression has side effects. \
                 This is not yet supported.",
                alias
            )));
        }

        self.volatile_expressions.insert(index);
        self.has_volatile = true;
        Ok(())
    }

    /// Mark an expression as containing a subquery.
    ///
    pub fn set_expression_has_subquery(&mut self, index: usize) {
        self.subquery_expressions.insert(index);
        self.has_subquery = true;
    }

    /// Check if an alias expression contains a subquery.
    ///
    pub fn alias_has_subquery(&self, index: usize) -> bool {
        self.subquery_expressions.contains(&index)
    }

    /// Add an expanded column with the given expansion count.
    ///
    ///
    /// This is used for UNNEST operations where a single SELECT item
    /// can expand into multiple columns (e.g., UNNEST(struct_column)).
    ///
    /// # Arguments
    /// * `expand_count` - The number of columns this expression expands to
    pub fn add_expanded_column(&mut self, expand_count: usize) {
        if self.expanded_column_indices.is_empty() {
            self.expanded_column_indices.push(0);
        }
        let last = *self.expanded_column_indices.last().unwrap();
        self.expanded_column_indices.push(last + expand_count);
    }

    /// Add a regular (non-expanded) column.
    ///
    ///
    /// This is equivalent to `add_expanded_column(1)`.
    pub fn add_regular_column(&mut self) {
        self.add_expanded_column(1);
    }

    /// Get the final projection index for an original SELECT list index.
    ///
    ///
    /// This maps the original SELECT list index to the final projection index
    /// after any UNNEST expansions have been applied.
    ///
    /// # Arguments
    /// * `index` - The original SELECT list index
    ///
    /// # Returns
    /// The final projection index, or the original index if no expansion tracking exists.
    pub fn get_final_index(&self, index: usize) -> usize {
        if index >= self.expanded_column_indices.len() {
            return index;
        }
        self.expanded_column_indices[index]
    }

    /// Check if an alias exists (case-insensitive).
    ///
    /// # Arguments
    /// * `alias` - The alias name to check
    ///
    /// # Returns
    /// `true` if the alias exists in the alias map.
    pub fn has_alias(&self, alias: &str) -> bool {
        self.alias_map.contains_key(alias) || self.alias_map.contains_key(&alias.to_lowercase())
    }

    /// Get the index for an alias (case-insensitive).
    ///
    /// # Arguments
    /// * `alias` - The alias name to look up
    ///
    /// # Returns
    /// The SELECT list index if the alias exists, `None` otherwise.
    pub fn get_alias_index(&self, alias: &str) -> Option<usize> {
        self.alias_map
            .get(alias)
            .copied()
            .or_else(|| self.alias_map.get(&alias.to_lowercase()).copied())
    }

    /// Add an alias mapping.
    ///
    /// # Arguments
    /// * `alias` - The alias name
    /// * `quoted` - Whether the alias was quoted in SQL source
    /// * `index` - The SELECT list index
    pub fn add_alias(&mut self, alias: &str, quoted: bool, index: usize) {
        let key = if quoted {
            alias.to_string()
        } else {
            alias.to_lowercase()
        };
        self.alias_map.insert(key, index);
    }

    /// Check if a projection exists for the given expression string.
    ///
    /// # Arguments
    /// * `expr_str` - The expression string representation
    ///
    /// # Returns
    /// The projection index if it exists, `None` otherwise.
    pub fn get_projection_index(&self, expr_str: &str) -> Option<usize> {
        self.projection_map.get(expr_str).copied()
    }

    /// Add a projection mapping.
    ///
    /// # Arguments
    /// * `expr_str` - The expression string representation
    /// * `index` - The projection index
    pub fn add_projection(&mut self, expr_str: String, index: usize) {
        self.projection_map.insert(expr_str, index);
    }

    /// Check if an expression at the given index is volatile.
    pub fn is_volatile(&self, index: usize) -> bool {
        self.volatile_expressions.contains(&index)
    }

    /// Check if an alias has been referenced.
    pub fn is_alias_referenced(&self, index: usize) -> bool {
        self.referenced_aliases.contains(&index)
    }

    /// Get the number of original expressions.
    pub fn expression_count(&self) -> usize {
        self.original_expressions.len()
    }

    /// Helper to get a display name for an expression at the given index.
    /// Used for error messages.
    fn get_expression_display_name(&self, index: usize) -> String {
        // First check if there's an alias in the alias_map
        for (alias, &idx) in &self.alias_map {
            if idx == index {
                return alias.clone();
            }
        }
        // Fall back to expression index
        format!("expression at index {}", index)
    }
}

#[cfg(test)]
mod tests {
    use super::{AliasLookup, SelectBindState};
    use paro_parser::ast::{Expr, Literal};

    fn literal_expr(v: u64) -> Expr {
        Expr::Literal {
            span: paro_parser::Span::default(),
            value: Literal::UInt64(v),
        }
    }

    #[test]
    fn unquoted_alias_lookup_is_case_insensitive() {
        let mut state = SelectBindState::new();
        state.add_alias("id", false, 0);
        state.original_expressions.push(literal_expr(1));

        let lookup = AliasLookup::snapshot(&state);
        assert_eq!(lookup.get_alias_index("id"), Some(0));
        assert_eq!(lookup.get_alias_index("ID"), Some(0));
    }

    #[test]
    fn quoted_alias_lookup_requires_exact_spelling() {
        let mut state = SelectBindState::new();
        state.add_alias("ID", true, 0);
        state.original_expressions.push(literal_expr(1));

        let lookup = AliasLookup::snapshot(&state);
        assert_eq!(lookup.get_alias_index("ID"), Some(0));
        assert_eq!(lookup.get_alias_index("id"), None);
    }
}
