// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate Function Definitions.
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅ `paro_common::vector`
//! - LogicalType: ✅ `paro_common::types`
//!

use paro_common::allocator::ArenaAllocator;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionRef, SelectionVector, Vector};
use std::fmt;
use std::sync::Arc;

pub mod distributive;

// Re-export FunctionData from scalar module
pub use crate::scalar::FunctionData;

/// Function to initialize the state.
/// The state is a raw byte slice of size `state_size`.
pub type AggregateInitializeFn = unsafe fn(state: *mut u8);

/// Whether `combine` may destructively modify the source state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateCombineType {
    PreserveInput,
    AllowDestructive,
}

/// Aggregate function call context shared across update/combine/finalize hooks.
pub struct AggregateInputData<'a> {
    /// Optional bind-time data copied onto the bound aggregate expression.
    pub bind_data: Option<&'a dyn FunctionData>,
    /// Arena for aggregate-local temporary allocations.
    pub allocator: &'a mut ArenaAllocator,
    /// Whether combine is allowed to destructively consume the source.
    pub combine_type: AggregateCombineType,
}

impl<'a> AggregateInputData<'a> {
    pub fn new(
        bind_data: Option<&'a dyn FunctionData>,
        allocator: &'a mut ArenaAllocator,
        combine_type: AggregateCombineType,
    ) -> Self {
        Self {
            bind_data,
            allocator,
            combine_type,
        }
    }
}

/// Zero-copy view over grouped aggregate state addresses.
///
/// Group lookup produces one base state address per input row. Aggregate
/// functions use this view to apply their state-layout offset lazily instead
/// of materializing another pointer vector for every aggregate in every batch.
pub struct AggregateStateInput<'a> {
    address_data: *const *mut u8,
    address_selection: SelectionRef<'a>,
    update_selection: Option<&'a SelectionVector>,
    state_offset: usize,
}

impl<'a> AggregateStateInput<'a> {
    pub fn try_new(
        addresses: &'a Vector,
        state_offset: usize,
        update_selection: Option<&'a SelectionVector>,
        count: usize,
    ) -> Result<Self> {
        if update_selection.is_some_and(|selection| count > selection.len()) {
            return Err(paro_common::error::internal(format!(
                "Aggregate state selection is shorter than the update: count={count}, selection={}",
                update_selection.map_or(0, SelectionVector::len)
            )));
        }
        if update_selection.is_none() && count > addresses.len() {
            return Err(paro_common::error::internal(format!(
                "Aggregate state address vector is shorter than the update: count={count}, addresses={}",
                addresses.len()
            )));
        }

        let address_view = addresses.try_to_view(addresses.len())?;
        let address_data = address_view.get_data::<*mut u8>().ok_or_else(|| {
            paro_common::error::internal(
                "Aggregate state addresses must use pointer-backed storage".to_string(),
            )
        })?;
        Ok(Self {
            address_data,
            address_selection: address_view.sel().clone(),
            update_selection,
            state_offset,
        })
    }

    /// Resolve the aggregate state for one logical update row.
    ///
    /// # Safety
    /// The caller must keep every base address alive and ensure `state_offset`
    /// identifies initialized storage for the aggregate function being called.
    #[inline]
    pub unsafe fn state_ptr(&self, row: usize) -> *mut u8 {
        let address_row = self
            .update_selection
            .map_or(row, |selection| selection.get(row));
        let physical_row = self.address_selection.get(address_row);
        (*self.address_data.add(physical_row)).add(self.state_offset)
    }
}

/// Function to update the state with new values.
///
/// # Arguments
/// * `inputs`: Input vectors (arguments to the aggregate function).
/// * `states`: Zero-copy view resolving the state for each row.
/// * `count`: Number of rows to process.
pub type AggregateUpdateFn = unsafe fn(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    states: &AggregateStateInput,
    count: usize,
);

/// Function to combine two states (merge source into target).
///
/// Use for parallel execution or merging hash table buckets.
///
/// # Arguments
/// * `source`: Vector containing pointers to source states.
/// * `target`: Vector containing pointers to target states.
/// * `count`: Number of pairs to combine.
pub type AggregateCombineFn =
    unsafe fn(source: &Vector, target: &Vector, input_data: &AggregateInputData, count: usize);

/// Function to finalize the state into a result.
///
/// # Arguments
/// * `states`: Vector containing pointers to states.
/// * `result`: Result vector to write to.
/// * `count`: Number of results to generate.
pub type AggregateFinalizeFn = unsafe fn(
    states: &Vector,
    input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) -> Result<()>;

/// Function to destruct complex states (if state contains allocated resources like StringHeap).
pub type AggregateDestructorFn =
    unsafe fn(states: &Vector, input_data: &AggregateInputData, count: usize);

/// Function to serialize one aggregate state into an engine-owned byte buffer.
///
/// This is used by build-phase aggregate spill for states that contain
/// owned heap objects and therefore cannot be byte-copied safely.
pub type AggregateStateSerializeFn = unsafe fn(
    state: *const u8,
    input_data: &AggregateInputData,
    output: &mut Vec<u8>,
) -> Result<()>;

/// Function to deserialize one aggregate state from bytes into uninitialized state memory.
///
/// Implementations must fully initialize `state` on success so the normal
/// aggregate `combine` and `destructor` hooks can be used afterwards.
pub type AggregateStateDeserializeFn =
    unsafe fn(input: &[u8], input_data: &AggregateInputData, state: *mut u8) -> Result<()>;

/// Function to simple update (for ungrouped aggregates).
/// The state is a single pointer, inputs are vectors.
pub type AggregateSimpleUpdateFn =
    unsafe fn(inputs: &[&Vector], input_data: &AggregateInputData, state: *mut u8, count: usize);

/// Update DISTINCT inputs that have already been partitioned into contiguous
/// group-state runs.
///
/// `states` contains one state per entry in `run_starts`; input vectors contain
/// `count` globally distinct argument rows. The callback can reduce each run
/// without materializing a repeated state pointer for every input row.
pub type AggregateDistinctRunUpdateFn = unsafe fn(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    states: &AggregateStateInput,
    run_starts: &[u32],
    count: usize,
);

// ============================================================================
// AggregateFunction
// ============================================================================

/// Definition of an aggregate function.
///
///
///
/// ## Fields
/// - `name`: Function name
/// - `arguments`: Fixed argument types
/// - `return_type`: Return type
/// - `state_size`: Size of the state in bytes
/// - `initialize`: Initialize the state
/// - `update`: Update the state (vectorized)
/// - `combine`: Combine states (vectorized)
/// - `finalize`: Finalize state to result (vectorized)
/// - `simple_update`: Simple update for ungrouped aggregation
/// - `destructor`: Destructor for complex states
/// - `varargs`: Optional type for variable arguments
/// - `bind_data`: Optional bind-time data
#[derive(Clone)]
pub struct AggregateFunction {
    pub name: String,
    pub arguments: Vec<LogicalType>,
    pub return_type: LogicalType,

    /// Size of the state in bytes.
    pub state_size: usize,

    /// Initialize the state.
    pub initialize: AggregateInitializeFn,

    /// Update the state (vectorized, for grouped aggregation).
    pub update: AggregateUpdateFn,

    /// Combine states (vectorized).
    pub combine: AggregateCombineFn,

    /// Finalize state to result (vectorized).
    pub finalize: AggregateFinalizeFn,

    /// Simple update (for ungrouped aggregation optimization).
    /// If None, the execution engine must use `update` with a vector of identical pointers.
    pub simple_update: Option<AggregateSimpleUpdateFn>,

    /// Optional reducer for pre-deduplicated, group-clustered input runs.
    pub distinct_run_update: Option<AggregateDistinctRunUpdateFn>,

    /// Destructor for the state (optional).
    pub destructor: Option<AggregateDestructorFn>,

    /// Optional serializer for complex aggregate states.
    pub state_serialize: Option<AggregateStateSerializeFn>,

    /// Optional deserializer for complex aggregate states.
    pub state_deserialize: Option<AggregateStateDeserializeFn>,

    /// Type for variable arguments (None = no varargs).
    /// When set, the function accepts any number of additional arguments of this type.
    pub varargs: Option<LogicalType>,

    /// Optional bind-time data.
    /// Stored during function binding and available during execution.
    pub bind_data: Option<Arc<dyn FunctionData>>,
}

impl fmt::Debug for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateFunction")
            .field("name", &self.name)
            .field("arguments", &self.arguments)
            .field("return_type", &self.return_type)
            .field("state_size", &self.state_size)
            .field("varargs", &self.varargs)
            .field("has_bind_data", &self.bind_data.is_some())
            .finish()
    }
}

impl AggregateFunction {
    pub fn new(
        name: String,
        arguments: Vec<LogicalType>,
        return_type: LogicalType,
        state_size: usize,
        initialize: AggregateInitializeFn,
        update: AggregateUpdateFn,
        combine: AggregateCombineFn,
        finalize: AggregateFinalizeFn,
        simple_update: Option<AggregateSimpleUpdateFn>,
        destructor: Option<AggregateDestructorFn>,
    ) -> Self {
        Self {
            name,
            arguments,
            return_type,
            state_size,
            initialize,
            update,
            combine,
            finalize,
            simple_update,
            distinct_run_update: None,
            destructor,
            state_serialize: None,
            state_deserialize: None,
            varargs: None,
            bind_data: None,
        }
    }

    /// Set the reducer used when DISTINCT finalization has contiguous group runs.
    pub fn with_distinct_run_update(mut self, update: AggregateDistinctRunUpdateFn) -> Self {
        self.distinct_run_update = Some(update);
        self
    }

    /// Set explicit state serialization hooks for build-phase spill.
    pub fn with_state_serialization(
        mut self,
        serialize: AggregateStateSerializeFn,
        deserialize: AggregateStateDeserializeFn,
    ) -> Self {
        self.state_serialize = Some(serialize);
        self.state_deserialize = Some(deserialize);
        self
    }

    /// Set varargs type for variable argument support.
    pub fn with_varargs(mut self, varargs_type: LogicalType) -> Self {
        self.varargs = Some(varargs_type);
        self
    }

    /// Set bind-time data.
    pub fn with_bind_data<T: FunctionData + 'static>(mut self, data: T) -> Self {
        self.bind_data = Some(Arc::new(data));
        self
    }

    /// Set bind-time data from Arc.
    pub fn with_bind_data_arc(mut self, data: Arc<dyn FunctionData>) -> Self {
        self.bind_data = Some(data);
        self
    }

    /// Check if this function accepts variable arguments.
    pub fn has_varargs(&self) -> bool {
        self.varargs.is_some()
    }

    /// Check if this function has bind data.
    pub fn has_bind_data(&self) -> bool {
        self.bind_data.is_some()
    }

    /// Get bind data as a specific type.
    pub fn get_bind_data<T: FunctionData + 'static>(&self) -> Option<&T> {
        self.bind_data
            .as_ref()
            .and_then(|d| d.as_any().downcast_ref::<T>())
    }
}

// ============================================================================
// AggregateFunctionSet
// ============================================================================

/// A set of aggregate functions with the same name but different signatures.
#[derive(Clone, Debug)]
pub struct AggregateFunctionSet {
    pub name: String,
    pub functions: Vec<AggregateFunction>,
    pub dynamic_bind: Option<AggregateFunctionSetBindFn>,
}

pub type AggregateFunctionSetBindFn =
    fn(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)>;

impl AggregateFunctionSet {
    pub fn new(name: String) -> Self {
        Self {
            name,
            functions: Vec::new(),
            dynamic_bind: None,
        }
    }

    pub fn add_function(&mut self, function: AggregateFunction) {
        self.functions.push(function);
    }

    pub fn set_dynamic_bind(&mut self, bind: AggregateFunctionSetBindFn) {
        self.dynamic_bind = Some(bind);
    }

    /// Find the best matching function for the given arguments using cost-based implicit casting.
    ///
    /// # Algorithm (aligned with ScalarFunctionSet::bind)
    /// 1. Find all candidate functions with matching argument count (or varargs)
    /// 2. For each candidate, calculate total cast cost using `CastRules::implicit_cast_cost`
    /// 3. Select the candidate with lowest total cost
    /// 4. If cost is -1 (impossible cast), skip that candidate
    ///
    /// # Varargs Support
    /// If a function has `varargs` set, it accepts:
    /// - At least `arguments.len()` arguments
    /// - Additional arguments must match the `varargs` type
    ///
    /// # Examples
    /// ```ignore
    /// // Given overloads: sum(INTEGER), sum(DOUBLE)
    /// // Call: sum(SMALLINT)
    /// // Result: sum(INTEGER) - cast SMALLINT to INTEGER (lower cost than DOUBLE)
    /// ```
    pub fn bind(&self, arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
        // Parameterized signatures such as DECIMAL(p,s) cannot be represented
        // by the fixed overload table. Prefer a dynamic match before considering
        // coercive fixed overloads (for example DECIMAL -> DOUBLE).
        let dynamic_error = if let Some(bind) = self.dynamic_bind {
            match bind(arguments) {
                Ok(bound) => return Ok(bound),
                Err(error) => Some(error),
            }
        } else {
            None
        };
        let mut best_match: Option<(&AggregateFunction, i64, Vec<LogicalType>)> = None;

        for func in &self.functions {
            let (is_valid, total_cost, target_types) = Self::calculate_bind_cost(func, arguments);

            if !is_valid {
                continue;
            }

            // Update best match if this is better (lower cost)
            match &best_match {
                None => {
                    best_match = Some((func, total_cost, target_types));
                }
                Some((_, best_cost, _)) if total_cost < *best_cost => {
                    best_match = Some((func, total_cost, target_types));
                }
                _ => {}
            }
        }

        match best_match {
            Some((func, _cost, target_types)) => Ok((func.clone(), target_types)),
            None => match dynamic_error {
                Some(error) => Err(error),
                None => Err(paro_common::error::catalog(format!(
                    "No matching aggregate function found for {} with arguments {:?}",
                    self.name, arguments
                ))),
            },
        }
    }

    /// Calculate the binding cost for a function with given arguments.
    ///
    /// Returns (is_valid, total_cost, target_types).
    fn calculate_bind_cost(
        func: &AggregateFunction,
        arguments: &[LogicalType],
    ) -> (bool, i64, Vec<LogicalType>) {
        use paro_common::cast_rules::CastRules;

        let fixed_count = func.arguments.len();
        let arg_count = arguments.len();

        // Check argument count compatibility
        if func.has_varargs() {
            // Varargs: need at least fixed_count arguments
            if arg_count < fixed_count {
                return (false, 0, Vec::new());
            }
        } else {
            // No varargs: exact match required
            if arg_count != fixed_count {
                return (false, 0, Vec::new());
            }
        }

        let mut total_cost: i64 = 0;
        let mut target_types = Vec::with_capacity(arg_count);

        // Process fixed arguments
        for (arg_type, param_type) in arguments.iter().take(fixed_count).zip(&func.arguments) {
            let cost = CastRules::implicit_cast_cost(arg_type, param_type);

            if cost < 0 {
                return (false, 0, Vec::new());
            }

            total_cost += cost;
            target_types.push(param_type.clone());
        }

        // Process varargs (if any)
        if let Some(ref varargs_type) = func.varargs {
            for arg_type in arguments.iter().skip(fixed_count) {
                let cost = CastRules::implicit_cast_cost(arg_type, varargs_type);

                if cost < 0 {
                    return (false, 0, Vec::new());
                }

                total_cost += cost;
                target_types.push(varargs_type.clone());
            }
        }

        (true, total_cost, target_types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn dummy_init(_state: *mut u8) {}
    unsafe fn dummy_update(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        _states: &AggregateStateInput,
        _count: usize,
    ) {
    }
    unsafe fn dummy_combine(
        _source: &Vector,
        _target: &Vector,
        _input_data: &AggregateInputData,
        _count: usize,
    ) {
    }
    unsafe fn dummy_finalize(
        _states: &Vector,
        _input_data: &AggregateInputData,
        _result: &mut Vector,
        _count: usize,
    ) -> Result<()> {
        Ok(())
    }

    fn create_dummy_aggregate(
        name: &str,
        args: Vec<LogicalType>,
        ret: LogicalType,
    ) -> AggregateFunction {
        AggregateFunction::new(
            name.to_string(),
            args,
            ret,
            8,
            dummy_init,
            dummy_update,
            dummy_combine,
            dummy_finalize,
            None,
            None,
        )
    }

    #[test]
    fn test_aggregate_function_set_bind_exact_match() {
        let mut set = AggregateFunctionSet::new("sum".to_string());

        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Integer],
            LogicalType::BigInt,
        ));
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Double],
            LogicalType::Double,
        ));

        // Test binding Integer - exact match
        let (matched, target_types) = set
            .bind(&[LogicalType::Integer])
            .expect("Should bind integer");
        assert_eq!(matched.return_type, LogicalType::BigInt);
        assert_eq!(target_types, vec![LogicalType::Integer]);

        // Test binding Double - exact match
        let (matched, target_types) = set
            .bind(&[LogicalType::Double])
            .expect("Should bind double");
        assert_eq!(matched.return_type, LogicalType::Double);
        assert_eq!(target_types, vec![LogicalType::Double]);
    }

    #[test]
    fn test_aggregate_function_set_bind_implicit_cast() {
        let mut set = AggregateFunctionSet::new("sum".to_string());

        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Integer],
            LogicalType::BigInt,
        ));
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Double],
            LogicalType::Double,
        ));

        // Test binding SmallInt - should cast to Integer (lower cost than Double)
        let (matched, target_types) = set
            .bind(&[LogicalType::SmallInt])
            .expect("Should bind with implicit cast");
        assert_eq!(matched.return_type, LogicalType::BigInt);
        assert_eq!(target_types, vec![LogicalType::Integer]);

        // Test binding BigInt - should cast to Double (Integer cannot hold BigInt)
        let (matched, target_types) = set
            .bind(&[LogicalType::BigInt])
            .expect("Should bind BigInt");
        // BigInt -> Integer has negative cost, BigInt -> Double is valid
        assert_eq!(matched.return_type, LogicalType::Double);
        assert_eq!(target_types, vec![LogicalType::Double]);
    }

    #[test]
    fn test_aggregate_function_set_bind_no_match() {
        let mut set = AggregateFunctionSet::new("sum".to_string());

        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Integer],
            LogicalType::BigInt,
        ));

        // Test mismatch - VARCHAR cannot cast to INTEGER
        let err = set.bind(&[LogicalType::Varchar]);
        assert!(err.is_err());

        // Test wrong argument count
        let err = set.bind(&[LogicalType::Integer, LogicalType::Integer]);
        assert!(err.is_err());
    }

    #[test]
    fn test_aggregate_function_varargs() {
        let func = create_dummy_aggregate("concat_agg", vec![], LogicalType::Varchar)
            .with_varargs(LogicalType::Varchar);

        assert!(func.has_varargs());
        assert_eq!(func.varargs, Some(LogicalType::Varchar));
    }

    #[test]
    fn test_aggregate_function_set_bind_varargs() {
        let mut set = AggregateFunctionSet::new("concat_agg".to_string());

        // concat_agg(VARCHAR...) - accepts any number of VARCHAR arguments
        let concat_varargs = create_dummy_aggregate("concat_agg", vec![], LogicalType::Varchar)
            .with_varargs(LogicalType::Varchar);

        set.add_function(concat_varargs);

        // Zero arguments
        let (_matched, target_types) = set.bind(&[]).expect("Should bind with 0 args");
        assert!(target_types.is_empty());

        // One argument
        let (_matched, target_types) = set
            .bind(&[LogicalType::Varchar])
            .expect("Should bind with 1 arg");
        assert_eq!(target_types, vec![LogicalType::Varchar]);

        // Three arguments
        let (_matched, target_types) = set
            .bind(&[
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ])
            .expect("Should bind with 3 args");
        assert_eq!(
            target_types,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar
            ]
        );
    }

    #[test]
    fn test_aggregate_function_set_bind_varargs_with_fixed() {
        let mut set = AggregateFunctionSet::new("string_agg".to_string());

        // string_agg(VARCHAR expr, VARCHAR separator, VARCHAR...) - fixed + varargs
        let string_agg = create_dummy_aggregate(
            "string_agg",
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Varchar,
        )
        .with_varargs(LogicalType::Varchar);

        set.add_function(string_agg);

        // Minimum arguments (expr + separator)
        let (_, target_types) = set
            .bind(&[LogicalType::Varchar, LogicalType::Varchar])
            .expect("Should bind with 2 args");
        assert_eq!(
            target_types,
            vec![LogicalType::Varchar, LogicalType::Varchar]
        );

        // With extra varargs
        let (_, target_types) = set
            .bind(&[
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ])
            .expect("Should bind with 3 args");
        assert_eq!(
            target_types,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar
            ]
        );

        // Too few arguments
        let err = set.bind(&[LogicalType::Varchar]);
        assert!(err.is_err());
    }

    #[test]
    fn test_aggregate_function_set_bind_best_match() {
        let mut set = AggregateFunctionSet::new("sum".to_string());

        // Add multiple overloads with different costs
        // Note: target_type_cost prefers wider types (BigInt=101, Integer=102, SmallInt=112)
        // So TinyInt -> BigInt (cost 101) is preferred over TinyInt -> Integer (cost 102)
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::SmallInt],
            LogicalType::BigInt,
        ));
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Integer],
            LogicalType::BigInt,
        ));
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
        ));
        set.add_function(create_dummy_aggregate(
            "sum",
            vec![LogicalType::Double],
            LogicalType::Double,
        ));

        // TinyInt should match BigInt (cost 101) - lowest cost target
        let (_matched, target_types) = set
            .bind(&[LogicalType::TinyInt])
            .expect("Should bind TinyInt");
        assert_eq!(target_types, vec![LogicalType::BigInt]);

        // Float should match Double
        let (_matched, target_types) = set.bind(&[LogicalType::Float]).expect("Should bind Float");
        assert_eq!(target_types, vec![LogicalType::Double]);
    }

    // ========== FunctionData Tests for AggregateFunction ==========

    use std::any::Any;

    /// Example bind data for AVG function (stores precision info)
    #[derive(Debug, Clone, PartialEq)]
    struct AvgBindData {
        precision: u8,
        scale: u8,
    }

    impl FunctionData for AvgBindData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }

        fn equals(&self, other: &dyn FunctionData) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map_or(false, |o| self == o)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_aggregate_function_with_bind_data() {
        let bind_data = AvgBindData {
            precision: 18,
            scale: 6,
        };

        let func = create_dummy_aggregate("avg", vec![LogicalType::Double], LogicalType::Double)
            .with_bind_data(bind_data.clone());

        assert!(func.has_bind_data());

        // Get bind data back
        let retrieved = func.get_bind_data::<AvgBindData>().unwrap();
        assert_eq!(retrieved.precision, 18);
        assert_eq!(retrieved.scale, 6);
    }

    #[test]
    fn test_aggregate_function_bind_data_clone() {
        let bind_data = AvgBindData {
            precision: 10,
            scale: 2,
        };

        let func = create_dummy_aggregate("avg", vec![LogicalType::Double], LogicalType::Double)
            .with_bind_data(bind_data);

        // Clone the function
        let cloned = func.clone();

        // Both should have bind data
        assert!(func.has_bind_data());
        assert!(cloned.has_bind_data());

        // Bind data should be equal
        let orig_data = func.get_bind_data::<AvgBindData>().unwrap();
        let cloned_data = cloned.get_bind_data::<AvgBindData>().unwrap();
        assert_eq!(orig_data.precision, cloned_data.precision);
        assert_eq!(orig_data.scale, cloned_data.scale);
    }

    #[test]
    fn test_aggregate_function_without_bind_data() {
        let func = create_dummy_aggregate("sum", vec![LogicalType::Integer], LogicalType::BigInt);

        assert!(!func.has_bind_data());
        assert!(func.get_bind_data::<AvgBindData>().is_none());
    }
}
