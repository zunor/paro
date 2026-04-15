//! Registration for built-in scalar, aggregate, table, and system functions.

use crate::builtin::table_functions::BuiltinTableFunctions;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::{
    AggregateFunctionCatalogEntry, CatalogEntryEnum, CatalogType, CopyFunctionCatalogEntry,
    ScalarFunctionCatalogEntry, SchemaEntry,
};
use paro_function::aggregate::{AggregateFunction, AggregateFunctionSet};
use paro_function::copy::register_copy_functions as register_copy_functions_builtin;
use paro_function::scalar::ScalarFunctionSet;
use std::sync::Arc;

/// Registers built-in functions into a schema.
pub struct BuiltinFunctions;

impl BuiltinFunctions {
    /// Register all built-in functions into the given schema.
    pub fn register_all(schema: &SchemaEntry) {
        // Core functions
        Self::register_scalar_functions(schema);
        Self::register_aggregate_functions(schema);
        Self::register_table_functions(schema);
        Self::register_copy_functions(schema);

        // PostgreSQL-compatible system functions
        Self::register_system_functions(schema);
    }

    /// Register pg_catalog system functions (internal).
    ///
    /// These are PostgreSQL-compatible system functions used by psql and other tools.
    fn register_system_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::system::register_system_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register all scalar functions.
    fn register_scalar_functions(schema: &SchemaEntry) {
        Self::register_arithmetic_functions(schema);
        Self::register_comparison_functions(schema);
        Self::register_logic_functions(schema);
        Self::register_case_functions(schema);
        Self::register_string_functions(schema);
        Self::register_date_functions(schema);
        Self::register_math_functions(schema);
        Self::register_null_functions(schema);
        Self::register_vector_functions(schema);
        Self::register_fulltext_functions(schema);
    }

    /// Register arithmetic operators: +, -, *, /, %
    fn register_arithmetic_functions(schema: &SchemaEntry) {
        let ops = ["+", "-", "*", "/", "%"];
        for op in ops {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut set);
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register comparison operators: =, !=, <, <=, >, >=
    fn register_comparison_functions(schema: &SchemaEntry) {
        let ops = ["=", "!=", "<", "<=", ">", ">="];
        for op in ops {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::comparison::register_comparison_functions(&mut set);
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register logic operators: and, or, not
    fn register_logic_functions(schema: &SchemaEntry) {
        let ops = ["and", "or", "not"];
        for op in ops {
            let mut set = ScalarFunctionSet::new(op.to_string());
            paro_function::scalar::operators::logic::register_logic_functions(&mut set);
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register case/conditional functions: if
    fn register_case_functions(schema: &SchemaEntry) {
        let mut if_set = ScalarFunctionSet::new("if".to_string());
        paro_function::scalar::operators::case::register_case_functions(&mut if_set);
        Self::register_scalar_set(schema, if_set);
    }

    /// Register string functions.
    fn register_string_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::string::register_string_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register date/time functions.
    fn register_date_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::date::register_date_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register math functions.
    fn register_math_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::math::register_math_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register NULL handling functions: ifnull, nullif, coalesce
    fn register_null_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::null_ops::register_null_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register vector functions: l2_distance, l1_distance, cosine_distance, etc.
    fn register_vector_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::vector::register_vector_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register full-text search functions: bm25, fulltext_match.
    fn register_fulltext_functions(schema: &SchemaEntry) {
        let function_sets = paro_function::scalar::fulltext::register_fulltext_functions();
        for set in function_sets {
            Self::register_scalar_set(schema, set);
        }
    }

    /// Register all aggregate functions.
    fn register_aggregate_functions(schema: &SchemaEntry) {
        Self::register_distributive_aggregates(schema);
    }

    /// Register distributive aggregate functions.
    /// These are aggregates that can be computed in parallel and combined.
    fn register_distributive_aggregates(schema: &SchemaEntry) {
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
        Self::register_aggregate_set(schema, get_count_function());

        // COUNT(*) - special case
        let count_star = get_count_star_function();
        let mut count_star_set = AggregateFunctionSet::new("count_star".to_string());
        count_star_set.add_function(count_star);
        Self::register_aggregate_set(schema, count_star_set);

        // SUM
        Self::register_aggregate_set(schema, get_sum_function());

        // MIN / MAX
        Self::register_aggregate_set(schema, get_min_function());
        Self::register_aggregate_set(schema, get_max_function());

        // AVG
        Self::register_aggregate_set(schema, get_avg_function());

        // FIRST / LAST
        Self::register_aggregate_set(schema, get_first_function());
        Self::register_aggregate_set(schema, get_last_function());
        Self::register_aggregate_set(schema, get_first_value_function());
        Self::register_aggregate_set(schema, get_last_value_function());

        // ANY_VALUE / ARBITRARY (aliases for FIRST)
        Self::register_aggregate_set(schema, get_any_value_function());
        Self::register_aggregate_set(schema, get_arbitrary_function());

        // STRING_AGG / ARRAY_AGG
        Self::register_aggregate_set(schema, get_string_agg_function());
        Self::register_aggregate_set(schema, get_array_agg_function());

        // Variance / Stddev family
        Self::register_aggregate_set(schema, get_var_pop_function());
        Self::register_aggregate_set(schema, get_var_samp_function());
        Self::register_aggregate_set(schema, get_variance_function());
        Self::register_aggregate_set(schema, get_stddev_pop_function());
        Self::register_aggregate_set(schema, get_stddev_samp_function());
        Self::register_aggregate_set(schema, get_stddev_function());

        // Boolean aggregates
        Self::register_aggregate_function(schema, get_bool_and_function());
        Self::register_aggregate_function(schema, get_bool_or_function());

        // Bitwise aggregates
        Self::register_aggregate_set(schema, get_bit_and_function());
        Self::register_aggregate_set(schema, get_bit_or_function());
        Self::register_aggregate_set(schema, get_bit_xor_function());
    }

    /// Register all table functions.
    ///
    /// Delegates to `BuiltinTableFunctions::register_all` which handles:
    /// - Core table functions: range, generate_series, unnest, repeat, repeat_row
    /// - System table functions: paro_schemas, paro_tables, paro_columns
    fn register_table_functions(schema: &SchemaEntry) {
        BuiltinTableFunctions::register_all(schema);
    }

    fn register_copy_functions(schema: &SchemaEntry) {
        for function in register_copy_functions_builtin() {
            let entry = Arc::new(CopyFunctionCatalogEntry::new(
                schema.base.catalog.clone(),
                schema.base.name.clone(),
                function,
                0,
            ));
            let _ = schema
                .collection(CatalogType::CopyFunction)
                .expect("copy function collection")
                .install_committed(
                    Arc::new(CatalogEntryEnum::CopyFunction(entry)),
                    InstallMode::RejectExisting,
                );
        }
    }

    /// Register a scalar function set into the schema.
    fn register_scalar_set(schema: &SchemaEntry, set: ScalarFunctionSet) {
        let entry = Arc::new(ScalarFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            0,
        ));
        let _ = schema
            .collection(CatalogType::ScalarFunction)
            .expect("function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::ScalarFunction(entry)),
                InstallMode::RejectExisting,
            );
    }

    /// Register an aggregate function set into the schema.
    fn register_aggregate_set(schema: &SchemaEntry, set: AggregateFunctionSet) {
        let entry = Arc::new(AggregateFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            0,
        ));
        let _ = schema
            .collection(CatalogType::AggregateFunction)
            .expect("function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::AggregateFunction(entry)),
                InstallMode::RejectExisting,
            );
    }

    /// Register a single aggregate function (wraps it in a set).
    fn register_aggregate_function(schema: &SchemaEntry, func: AggregateFunction) {
        let name = func.name.clone();
        let mut set = AggregateFunctionSet::new(name);
        set.add_function(func);
        Self::register_aggregate_set(schema, set);
    }
}
