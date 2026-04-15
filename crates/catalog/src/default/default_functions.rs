// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Default Functions - Function Definitions and Generator
//!
//!
//!
//! ## Overview
//!
//! `DefaultFunctionGenerator` provides lazy initialization for built-in functions
//! in each schema:
//! - `public`: All standard scalar, aggregate, and table functions
//! - `pg_catalog`: PostgreSQL-compatible system functions

use super::DefaultGenerator;
use crate::entry::{
    AggregateFunctionCatalogEntry, CatalogEntryEnum, ScalarFunctionCatalogEntry,
    TableFunctionCatalogEntry,
};
use paro_function::aggregate::AggregateFunctionSet;
use paro_function::scalar::ScalarFunctionSet;
use paro_function::table::TableFunctionSet;
use std::collections::HashMap;
use std::sync::Arc;

/// Function definition type
#[derive(Clone)]
enum FunctionDef {
    Scalar(ScalarFunctionSet),
    Aggregate(AggregateFunctionSet),
}

/// Default generator for built-in functions within a specific schema.
///
/// Each schema (public, pg_catalog) has its own set of default functions.
/// This generator creates functions on-demand when first accessed.
pub struct DefaultFunctionGenerator {
    /// The catalog name (database name) for created functions
    catalog_name: String,
    /// The schema name this generator is responsible for
    schema_name: String,
    /// Cached function definitions (name -> definition)
    functions: HashMap<String, FunctionDef>,
}

impl DefaultFunctionGenerator {
    /// Create a new DefaultFunctionGenerator for the given catalog and schema.
    pub fn new(catalog_name: String, schema_name: String) -> Self {
        let functions = Self::build_function_map(&schema_name);
        Self {
            catalog_name,
            schema_name,
            functions,
        }
    }

    /// Build the function map for the given schema.
    fn build_function_map(schema_name: &str) -> HashMap<String, FunctionDef> {
        let mut map = HashMap::new();

        match schema_name.to_lowercase().as_str() {
            "public" => {
                Self::add_public_functions(&mut map);
            }
            "pg_catalog" => {
                Self::add_pg_catalog_functions(&mut map);
            }
            _ => {
                // Unknown schema - no default functions
            }
        }

        map
    }

    /// Add all public schema functions to the map.
    fn add_public_functions(map: &mut HashMap<String, FunctionDef>) {
        // Scalar functions
        Self::add_scalar_functions(map);

        // Aggregate functions
        Self::add_aggregate_functions(map);
    }

    /// Add scalar functions to the map.
    fn add_scalar_functions(map: &mut HashMap<String, FunctionDef>) {
        // Arithmetic operators
        for op in ["+", "-", "*", "/", "%"] {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut set);
            map.insert(op.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Comparison operators
        for op in ["=", "!=", "<", "<=", ">", ">="] {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::comparison::register_comparison_functions(&mut set);
            map.insert(op.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Logic operators
        for op in ["and", "or", "not"] {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::logic::register_logic_functions(&mut set);
            map.insert(op.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Case/conditional functions
        let mut if_set = ScalarFunctionSet::new("if".to_string());
        paro_function::scalar::operators::case::register_case_functions(&mut if_set);
        map.insert("if".to_string(), FunctionDef::Scalar(if_set));

        // String functions
        for set in paro_function::scalar::string::register_string_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Date functions
        for set in paro_function::scalar::date::register_date_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Math functions
        for set in paro_function::scalar::math::register_math_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }

        // NULL handling functions
        for set in paro_function::scalar::null_ops::register_null_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Vector functions: l2_distance/cosine_distance/sparse_distance/...
        for set in paro_function::scalar::vector::register_vector_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }

        // Full-text functions: bm25/fulltext_match + internal planner helpers
        for set in paro_function::scalar::fulltext::register_fulltext_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }
    }

    /// Add aggregate functions to the map.
    fn add_aggregate_functions(map: &mut HashMap<String, FunctionDef>) {
        use paro_function::aggregate::distributive::{
            array_agg::get_array_agg_function,
            avg::get_avg_function,
            bit_agg::{get_bit_and_function, get_bit_or_function, get_bit_xor_function},
            bool_agg::{get_bool_and_function, get_bool_or_function},
            count::{get_count_function, get_count_star_function},
            first_last::{
                get_any_value_function, get_arbitrary_function, get_first_function,
                get_first_value_function, get_last_function, get_last_value_function,
            },
            minmax::{get_max_function, get_min_function},
            string_agg::get_string_agg_function,
            sum::get_sum_function,
            variance::{
                get_stddev_function, get_stddev_pop_function, get_stddev_samp_function,
                get_var_pop_function, get_var_samp_function, get_variance_function,
            },
        };

        // COUNT
        let count_set = get_count_function();
        map.insert("count".to_string(), FunctionDef::Aggregate(count_set));

        let mut count_star_set = AggregateFunctionSet::new("count_star".to_string());
        count_star_set.add_function(get_count_star_function());
        map.insert(
            "count_star".to_string(),
            FunctionDef::Aggregate(count_star_set),
        );

        // SUM
        let sum_set = get_sum_function();
        map.insert("sum".to_string(), FunctionDef::Aggregate(sum_set));

        // MIN/MAX
        let min_set = get_min_function();
        map.insert("min".to_string(), FunctionDef::Aggregate(min_set));
        let max_set = get_max_function();
        map.insert("max".to_string(), FunctionDef::Aggregate(max_set));

        // AVG
        let avg_set = get_avg_function();
        map.insert("avg".to_string(), FunctionDef::Aggregate(avg_set));

        // BIT_AND, BIT_OR, BIT_XOR
        let bit_and_set = get_bit_and_function();
        map.insert("bit_and".to_string(), FunctionDef::Aggregate(bit_and_set));
        let bit_or_set = get_bit_or_function();
        map.insert("bit_or".to_string(), FunctionDef::Aggregate(bit_or_set));
        let bit_xor_set = get_bit_xor_function();
        map.insert("bit_xor".to_string(), FunctionDef::Aggregate(bit_xor_set));

        // BOOL_AND, BOOL_OR
        let mut bool_and_set = AggregateFunctionSet::new("bool_and".to_string());
        bool_and_set.add_function(get_bool_and_function());
        map.insert("bool_and".to_string(), FunctionDef::Aggregate(bool_and_set));

        let mut bool_or_set = AggregateFunctionSet::new("bool_or".to_string());
        bool_or_set.add_function(get_bool_or_function());
        map.insert("bool_or".to_string(), FunctionDef::Aggregate(bool_or_set));

        // FIRST, LAST, ANY_VALUE
        let first_set = get_first_function();
        map.insert("first".to_string(), FunctionDef::Aggregate(first_set));
        let last_set = get_last_function();
        map.insert("last".to_string(), FunctionDef::Aggregate(last_set));
        let first_value_set = get_first_value_function();
        map.insert(
            "first_value".to_string(),
            FunctionDef::Aggregate(first_value_set),
        );
        let last_value_set = get_last_value_function();
        map.insert(
            "last_value".to_string(),
            FunctionDef::Aggregate(last_value_set),
        );
        let any_value_set = get_any_value_function();
        map.insert(
            "any_value".to_string(),
            FunctionDef::Aggregate(any_value_set),
        );
        let arbitrary_set = get_arbitrary_function();
        map.insert(
            "arbitrary".to_string(),
            FunctionDef::Aggregate(arbitrary_set),
        );

        // STRING_AGG / ARRAY_AGG
        let string_agg_set = get_string_agg_function();
        map.insert(
            "string_agg".to_string(),
            FunctionDef::Aggregate(string_agg_set),
        );
        let array_agg_set = get_array_agg_function();
        map.insert(
            "array_agg".to_string(),
            FunctionDef::Aggregate(array_agg_set),
        );

        // Variance / Stddev family
        let var_pop_set = get_var_pop_function();
        map.insert("var_pop".to_string(), FunctionDef::Aggregate(var_pop_set));
        let var_samp_set = get_var_samp_function();
        map.insert("var_samp".to_string(), FunctionDef::Aggregate(var_samp_set));
        let variance_set = get_variance_function();
        map.insert("variance".to_string(), FunctionDef::Aggregate(variance_set));
        let stddev_pop_set = get_stddev_pop_function();
        map.insert(
            "stddev_pop".to_string(),
            FunctionDef::Aggregate(stddev_pop_set),
        );
        let stddev_samp_set = get_stddev_samp_function();
        map.insert(
            "stddev_samp".to_string(),
            FunctionDef::Aggregate(stddev_samp_set),
        );
        let stddev_set = get_stddev_function();
        map.insert("stddev".to_string(), FunctionDef::Aggregate(stddev_set));
    }

    /// Add pg_catalog schema functions to the map.
    fn add_pg_catalog_functions(map: &mut HashMap<String, FunctionDef>) {
        // Register system functions
        for set in paro_function::scalar::system::register_system_functions() {
            map.insert(set.name.to_lowercase(), FunctionDef::Scalar(set));
        }
    }
}

impl DefaultGenerator for DefaultFunctionGenerator {
    fn is_default_entry(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
        let lower_name = name.to_lowercase();
        let def = self.functions.get(&lower_name)?;

        match def {
            FunctionDef::Scalar(set) => {
                let entry = ScalarFunctionCatalogEntry::new(
                    self.catalog_name.clone(),
                    self.schema_name.clone(),
                    set.clone(),
                    0, // timestamp = 0
                );
                Some(Arc::new(CatalogEntryEnum::ScalarFunction(Arc::new(entry))))
            }
            FunctionDef::Aggregate(set) => {
                let entry = AggregateFunctionCatalogEntry::new(
                    self.catalog_name.clone(),
                    self.schema_name.clone(),
                    set.clone(),
                    0, // timestamp = 0
                );
                Some(Arc::new(CatalogEntryEnum::AggregateFunction(Arc::new(
                    entry,
                ))))
            }
        }
    }

    fn get_default_entries(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
}

// ============================================================================
// DefaultTableFunctionGenerator
// ============================================================================

/// Default generator for table functions.
///
/// This is separate from DefaultFunctionGenerator because table functions
/// are stored in a different catalog set (`table_functions` vs `functions`).
pub struct DefaultTableFunctionGenerator {
    catalog_name: String,
    schema_name: String,
    functions: HashMap<String, TableFunctionSet>,
}

impl DefaultTableFunctionGenerator {
    pub fn new(catalog_name: String, schema_name: String) -> Self {
        let mut functions = HashMap::new();

        // Register table functions for public and pg_catalog schemas
        if schema_name.eq_ignore_ascii_case("public")
            || schema_name.eq_ignore_ascii_case("pg_catalog")
        {
            for set in Self::get_all_table_function_sets() {
                functions.insert(set.name.to_lowercase(), set);
            }
        }

        Self {
            catalog_name,
            schema_name,
            functions,
        }
    }

    /// Get all table function sets.
    ///
    /// This directly references paro_function::table functions.
    fn get_all_table_function_sets() -> Vec<TableFunctionSet> {
        use paro_function::table::range::{
            create_generate_series_function_set, create_range_function_set,
        };
        use paro_function::table::repeat::{
            create_repeat_function_set, create_repeat_row_function_set,
        };
        use paro_function::table::system::{
            create_paro_columns_function_set, create_paro_databases_function_set,
            create_paro_graph_statistics_function_set, create_paro_indexes_function_set,
            create_paro_logs_function_set, create_paro_memory_function_set,
            create_paro_optimizers_function_set, create_paro_pg_cursors_function_set,
            create_paro_pg_prepared_statements_function_set, create_paro_pg_settings_function_set,
            create_paro_property_graphs_function_set, create_paro_schemas_function_set,
            create_paro_storage_info_function_set, create_paro_tables_function_set,
            create_paro_temporary_files_function_set, create_paro_views_function_set,
            create_pragma_database_size_function_set,
        };
        use paro_function::table::unnest::create_unnest_function_set;

        vec![
            // Core table functions
            create_range_function_set(),
            create_generate_series_function_set(),
            create_unnest_function_set(),
            create_repeat_function_set(),
            create_repeat_row_function_set(),
            // System table functions
            create_paro_databases_function_set(),
            create_paro_schemas_function_set(),
            create_paro_tables_function_set(),
            create_paro_columns_function_set(),
            create_paro_views_function_set(),
            create_paro_indexes_function_set(),
            create_paro_pg_settings_function_set(),
            create_paro_pg_prepared_statements_function_set(),
            create_paro_pg_cursors_function_set(),
            create_paro_logs_function_set(),
            create_paro_memory_function_set(),
            create_paro_optimizers_function_set(),
            create_paro_storage_info_function_set(),
            create_paro_temporary_files_function_set(),
            create_pragma_database_size_function_set(),
            // Graph system table functions
            create_paro_property_graphs_function_set(),
            create_paro_graph_statistics_function_set(),
        ]
    }
}

impl DefaultGenerator for DefaultTableFunctionGenerator {
    fn is_default_entry(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
        let lower_name = name.to_lowercase();
        let set = self.functions.get(&lower_name)?;

        let entry = TableFunctionCatalogEntry::new(
            self.catalog_name.clone(),
            self.schema_name.clone(),
            set.clone(),
            0, // timestamp = 0
        );
        Some(Arc::new(CatalogEntryEnum::TableFunction(Arc::new(entry))))
    }

    fn get_default_entries(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultFunctionGenerator, FunctionDef};

    #[test]
    fn public_defaults_include_vector_and_fulltext_functions() {
        let map = DefaultFunctionGenerator::build_function_map("public");
        assert!(matches!(
            map.get("l2_distance"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("sparse_distance"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(map.get("bm25"), Some(FunctionDef::Scalar(_))));
        assert!(matches!(
            map.get("fulltext_match"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("to_tsvector"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("to_tsquery"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("plainto_tsquery"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("phraseto_tsquery"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("websearch_to_tsquery"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(map.get("ts_rank"), Some(FunctionDef::Scalar(_))));
        assert!(matches!(
            map.get("ts_rank_cd"),
            Some(FunctionDef::Scalar(_))
        ));
        assert!(matches!(
            map.get("ts_headline"),
            Some(FunctionDef::Scalar(_))
        ));
    }

    #[test]
    fn pg_catalog_defaults_do_not_include_search_functions() {
        let map = DefaultFunctionGenerator::build_function_map("pg_catalog");
        assert!(!map.contains_key("bm25"));
        assert!(!map.contains_key("sparse_distance"));
    }
}
