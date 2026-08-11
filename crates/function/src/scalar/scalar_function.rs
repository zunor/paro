// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{self, Debug};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::expression_state::ExpressionState;
use super::function_data::FunctionData;
use super::local_state::FunctionLocalState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FunctionStability {
    #[default]
    Consistent,
    ConsistentWithinQuery,
    Volatile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FunctionNullHandling {
    #[default]
    DefaultNullHandling,
    SpecialHandling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FunctionSideEffects {
    #[default]
    NoSideEffects,
    HasSideEffects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FunctionErrorMode {
    #[default]
    CanError,
    Infallible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DictionaryStrategy {
    #[default]
    Materialize,
    StorageDictionaryCache {
        input_idx: usize,
    },
}

pub type ScalarFunctionFn = fn(&Chunk, &dyn ExpressionState, &mut Vector) -> Result<()>;
pub type InitLocalStateFn =
    fn(&dyn ExpressionState, Option<&dyn FunctionData>) -> Result<Box<dyn FunctionLocalState>>;

#[derive(Clone, Copy)]
pub enum ScalarDispatch {
    Direct(ScalarFunctionFn),
    Variadic(ScalarFunctionFn),
}

impl ScalarDispatch {
    pub fn execute(
        &self,
        input: &Chunk,
        state: &dyn ExpressionState,
        result: &mut Vector,
    ) -> Result<()> {
        match self {
            Self::Direct(function) | Self::Variadic(function) => function(input, state, result),
        }
    }
}

impl Debug for ScalarDispatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(function) => f
                .debug_tuple("Direct")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
            Self::Variadic(function) => f
                .debug_tuple("Variadic")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScalarBindInput {
    pub argument_types: Vec<LogicalType>,
    pub constant_values: Vec<Option<Value>>,
}

impl ScalarBindInput {
    pub fn new(argument_types: Vec<LogicalType>, constant_values: Vec<Option<Value>>) -> Self {
        debug_assert_eq!(argument_types.len(), constant_values.len());
        Self {
            argument_types,
            constant_values,
        }
    }

    pub fn constant_value(&self, index: usize) -> Option<&Value> {
        self.constant_values.get(index).and_then(Option::as_ref)
    }
}

pub type ScalarBindFn =
    fn(function: &ScalarFunction, input: &ScalarBindInput) -> Result<BoundScalarFunction>;

#[derive(Clone)]
pub struct ScalarFunction {
    pub name: String,
    pub arguments: Vec<LogicalType>,
    pub return_type: LogicalType,
    pub dispatch: ScalarDispatch,
    pub bind: Option<ScalarBindFn>,
    pub init_local_state: Option<InitLocalStateFn>,
    pub stability: FunctionStability,
    pub null_handling: FunctionNullHandling,
    pub side_effects: FunctionSideEffects,
    pub varargs: Option<LogicalType>,
}

impl Debug for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunction")
            .field("name", &self.name)
            .field("arguments", &self.arguments)
            .field("return_type", &self.return_type)
            .field("dispatch", &self.dispatch)
            .field("has_bind", &self.bind.is_some())
            .field("has_init_local_state", &self.init_local_state.is_some())
            .field("stability", &self.stability)
            .field("null_handling", &self.null_handling)
            .field("side_effects", &self.side_effects)
            .field("varargs", &self.varargs)
            .finish()
    }
}

impl ScalarFunction {
    pub fn new(
        name: String,
        arguments: Vec<LogicalType>,
        return_type: LogicalType,
        function: ScalarFunctionFn,
    ) -> Self {
        Self {
            name,
            arguments,
            return_type,
            dispatch: ScalarDispatch::Direct(function),
            bind: None,
            init_local_state: None,
            stability: FunctionStability::Consistent,
            null_handling: FunctionNullHandling::DefaultNullHandling,
            side_effects: FunctionSideEffects::NoSideEffects,
            varargs: None,
        }
    }

    pub fn with_dispatch(mut self, dispatch: ScalarDispatch) -> Self {
        self.dispatch = dispatch;
        self
    }

    pub fn with_bind(mut self, bind: ScalarBindFn) -> Self {
        self.bind = Some(bind);
        self
    }

    pub fn with_init_local_state(mut self, init_local_state: InitLocalStateFn) -> Self {
        self.init_local_state = Some(init_local_state);
        self
    }

    pub fn with_stability(mut self, stability: FunctionStability) -> Self {
        self.stability = stability;
        self
    }

    pub fn with_null_handling(mut self, null_handling: FunctionNullHandling) -> Self {
        self.null_handling = null_handling;
        self
    }

    pub fn with_side_effects(mut self, side_effects: FunctionSideEffects) -> Self {
        self.side_effects = side_effects;
        self
    }

    pub fn with_varargs(mut self, varargs_type: LogicalType) -> Self {
        self.varargs = Some(varargs_type);
        self
    }

    pub fn has_varargs(&self) -> bool {
        self.varargs.is_some()
    }

    pub fn bind(&self, input: &ScalarBindInput) -> Result<BoundScalarFunction> {
        match self.bind {
            Some(bind) => bind(self, input),
            None => Ok(self.clone().into()),
        }
    }
}

#[derive(Clone)]
pub struct BoundScalarFunction {
    pub name: String,
    pub arguments: Vec<LogicalType>,
    pub return_type: LogicalType,
    pub dispatch: ScalarDispatch,
    pub init_local_state: Option<InitLocalStateFn>,
    pub stability: FunctionStability,
    pub null_handling: FunctionNullHandling,
    pub side_effects: FunctionSideEffects,
    pub varargs: Option<LogicalType>,
    pub bind_data: Option<Arc<dyn FunctionData>>,
    pub error_mode: FunctionErrorMode,
    pub dictionary_strategy: DictionaryStrategy,
}

impl Debug for BoundScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundScalarFunction")
            .field("name", &self.name)
            .field("arguments", &self.arguments)
            .field("return_type", &self.return_type)
            .field("dispatch", &self.dispatch)
            .field("has_init_local_state", &self.init_local_state.is_some())
            .field("stability", &self.stability)
            .field("null_handling", &self.null_handling)
            .field("side_effects", &self.side_effects)
            .field("varargs", &self.varargs)
            .field("has_bind_data", &self.bind_data.is_some())
            .field("error_mode", &self.error_mode)
            .field("dictionary_strategy", &self.dictionary_strategy)
            .finish()
    }
}

impl From<ScalarFunction> for BoundScalarFunction {
    fn from(function: ScalarFunction) -> Self {
        Self {
            name: function.name,
            arguments: function.arguments,
            return_type: function.return_type,
            dispatch: function.dispatch,
            init_local_state: function.init_local_state,
            stability: function.stability,
            null_handling: function.null_handling,
            side_effects: function.side_effects,
            varargs: function.varargs,
            bind_data: None,
            error_mode: FunctionErrorMode::CanError,
            dictionary_strategy: DictionaryStrategy::Materialize,
        }
    }
}

impl BoundScalarFunction {
    pub fn execute(
        &self,
        input: &Chunk,
        state: &dyn ExpressionState,
        result: &mut Vector,
    ) -> Result<()> {
        self.dispatch.execute(input, state, result)
    }

    pub fn with_dispatch(mut self, dispatch: ScalarDispatch) -> Self {
        self.dispatch = dispatch;
        self
    }

    pub fn with_bind_data<T: FunctionData + 'static>(mut self, data: T) -> Self {
        self.bind_data = Some(Arc::new(data));
        self
    }

    pub fn with_bind_data_arc(mut self, data: Arc<dyn FunctionData>) -> Self {
        self.bind_data = Some(data);
        self
    }

    pub fn with_init_local_state(mut self, init_local_state: InitLocalStateFn) -> Self {
        self.init_local_state = Some(init_local_state);
        self
    }

    pub fn with_error_mode(mut self, error_mode: FunctionErrorMode) -> Self {
        self.error_mode = error_mode;
        self
    }

    pub fn with_dictionary_strategy(mut self, dictionary_strategy: DictionaryStrategy) -> Self {
        self.dictionary_strategy = dictionary_strategy;
        self
    }

    pub fn has_bind_data(&self) -> bool {
        self.bind_data.is_some()
    }

    pub fn get_bind_data<T: FunctionData + 'static>(&self) -> Option<&T> {
        self.bind_data
            .as_ref()
            .and_then(|data| data.as_any().downcast_ref::<T>())
    }
}

#[derive(Clone, Debug)]
pub struct ScalarFunctionSet {
    pub name: String,
    pub functions: Vec<ScalarFunction>,
    pub dynamic_bind: Option<ScalarFunctionSetBindFn>,
}

pub type ScalarFunctionSetBindFn =
    fn(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)>;

impl ScalarFunctionSet {
    pub fn new(name: String) -> Self {
        Self {
            name,
            functions: Vec::new(),
            dynamic_bind: None,
        }
    }

    pub fn add_function(&mut self, function: ScalarFunction) {
        self.functions.push(function);
    }

    pub fn set_dynamic_bind(&mut self, bind: ScalarFunctionSetBindFn) {
        self.dynamic_bind = Some(bind);
    }

    pub fn bind(&self, arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
        // Dynamic binders model parameterized signatures (notably DECIMAL(p,s)).
        // Give them first refusal so a coercive fixed signature such as
        // DOUBLE,DOUBLE cannot erase the parameterized type semantics.
        let dynamic_error = if let Some(bind) = self.dynamic_bind {
            match bind(arguments) {
                Ok(bound) => return Ok(bound),
                Err(error) => Some(error),
            }
        } else {
            None
        };
        let mut best_match: Option<(&ScalarFunction, i64, Vec<LogicalType>)> = None;

        for func in &self.functions {
            let (is_valid, total_cost, mut target_types) =
                Self::calculate_bind_cost(func, arguments);
            if !is_valid {
                continue;
            }

            let mut known_array_size: Option<usize> = None;
            let mut known_array_child_type: Option<Box<LogicalType>> = None;
            let mut known_list_child_type: Option<Box<LogicalType>> = None;

            for (i, target_type) in target_types.iter().enumerate() {
                if let LogicalType::Array(t_child, 0) = target_type {
                    if let LogicalType::Array(a_child, a_size) = &arguments[i] {
                        if *a_size > 0 {
                            if known_array_size.is_none() || known_array_size == Some(*a_size) {
                                known_array_size = Some(*a_size);
                                if matches!(**t_child, LogicalType::Unknown) || *t_child == *a_child
                                {
                                    known_array_child_type = Some(a_child.clone());
                                }
                            }
                        }
                    }
                }

                if let LogicalType::List(t_child) = target_type {
                    if let LogicalType::List(a_child) = &arguments[i] {
                        if matches!(**t_child, LogicalType::Unknown) || *t_child == *a_child {
                            if known_list_child_type.is_none()
                                || known_list_child_type.as_ref() == Some(a_child)
                            {
                                known_list_child_type = Some(a_child.clone());
                            }
                        }
                    }
                }
            }

            for (i, target_type) in target_types.iter_mut().enumerate() {
                match (&*target_type, &arguments[i]) {
                    (LogicalType::Array(t_child, 0), LogicalType::Array(a_child, a_size)) => {
                        if *t_child == *a_child || matches!(**t_child, LogicalType::Unknown) {
                            *target_type = LogicalType::Array(a_child.clone(), *a_size);
                        }
                    }
                    (
                        LogicalType::Array(t_child, 0),
                        LogicalType::StringLiteral | LogicalType::Varchar,
                    ) => {
                        if let Some(size) = known_array_size {
                            let child = known_array_child_type
                                .clone()
                                .unwrap_or_else(|| t_child.clone());
                            *target_type = LogicalType::Array(child, size);
                        }
                    }
                    (LogicalType::List(t_child), LogicalType::List(a_child)) => {
                        if *t_child == *a_child || matches!(**t_child, LogicalType::Unknown) {
                            *target_type = LogicalType::List(a_child.clone());
                        }
                    }
                    (
                        LogicalType::List(t_child),
                        LogicalType::StringLiteral | LogicalType::Varchar,
                    ) => {
                        if let Some(child) = known_list_child_type.clone() {
                            *target_type = LogicalType::List(child);
                        } else if !matches!(**t_child, LogicalType::Unknown) {
                            *target_type = LogicalType::List(t_child.clone());
                        }
                    }
                    _ => {}
                }
            }

            match &best_match {
                None => best_match = Some((func, total_cost, target_types)),
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
                None => Err(paro_error::function_not_found(format!(
                    "{} with arguments {:?}",
                    self.name, arguments
                ))),
            },
        }
    }

    fn calculate_bind_cost(
        func: &ScalarFunction,
        arguments: &[LogicalType],
    ) -> (bool, i64, Vec<LogicalType>) {
        use paro_common::cast_rules::CastRules;

        let fixed_count = func.arguments.len();
        let arg_count = arguments.len();

        if func.has_varargs() {
            if arg_count < fixed_count {
                return (false, 0, Vec::new());
            }
        } else if arg_count != fixed_count {
            return (false, 0, Vec::new());
        }

        let mut total_cost = 0;
        let mut target_types = Vec::with_capacity(arg_count);

        for (arg_type, param_type) in arguments.iter().take(fixed_count).zip(&func.arguments) {
            let cost = CastRules::implicit_cast_cost(arg_type, param_type);
            if cost < 0 {
                return (false, 0, Vec::new());
            }
            total_cost += cost;
            target_types.push(param_type.clone());
        }

        if let Some(varargs_type) = &func.varargs {
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
    use std::any::Any;

    use super::*;

    fn dummy_function(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Hash)]
    struct TestBindData {
        value: i32,
    }

    impl FunctionData for TestBindData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }

        fn equals(&self, other: &dyn FunctionData) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| other == self)
        }

        fn fingerprint(&self) -> u64 {
            crate::scalar::function_data_fingerprint(self)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn bind_with_constant(
        function: &ScalarFunction,
        input: &ScalarBindInput,
    ) -> Result<BoundScalarFunction> {
        let value = input
            .constant_value(0)
            .and_then(Value::as_i64)
            .ok_or_else(|| paro_error::internal("missing bound constant".to_string()))?;
        Ok(
            BoundScalarFunction::from(function.clone()).with_bind_data(TestBindData {
                value: value as i32,
            }),
        )
    }

    #[test]
    fn function_enums_default() {
        assert_eq!(FunctionStability::default(), FunctionStability::Consistent);
        assert_eq!(
            FunctionNullHandling::default(),
            FunctionNullHandling::DefaultNullHandling
        );
        assert_eq!(
            FunctionSideEffects::default(),
            FunctionSideEffects::NoSideEffects
        );
        assert_eq!(FunctionErrorMode::default(), FunctionErrorMode::CanError);
        assert_eq!(
            DictionaryStrategy::default(),
            DictionaryStrategy::Materialize
        );
    }

    #[test]
    fn scalar_function_builder() {
        let func = ScalarFunction::new(
            "test".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            dummy_function,
        )
        .with_stability(FunctionStability::Volatile)
        .with_null_handling(FunctionNullHandling::SpecialHandling)
        .with_side_effects(FunctionSideEffects::HasSideEffects)
        .with_varargs(LogicalType::Varchar);

        assert_eq!(func.name, "test");
        assert_eq!(func.stability, FunctionStability::Volatile);
        assert_eq!(func.null_handling, FunctionNullHandling::SpecialHandling);
        assert_eq!(func.side_effects, FunctionSideEffects::HasSideEffects);
        assert!(func.has_varargs());
    }

    #[test]
    fn scalar_function_bind_builds_bound_function() {
        let func = ScalarFunction::new(
            "test".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            dummy_function,
        )
        .with_bind(bind_with_constant);
        let input =
            ScalarBindInput::new(vec![LogicalType::Integer], vec![Some(Value::Integer(42))]);

        let bound = func.bind(&input).expect("bind scalar function");

        assert!(bound.has_bind_data());
        assert_eq!(
            bound.get_bind_data::<TestBindData>().map(|data| data.value),
            Some(42)
        );
    }

    #[test]
    fn scalar_function_set_bind_exact_match() {
        let mut set = ScalarFunctionSet::new("add".to_string());
        set.add_function(ScalarFunction::new(
            "add".to_string(),
            vec![LogicalType::Integer, LogicalType::Integer],
            LogicalType::Integer,
            dummy_function,
        ));
        set.add_function(ScalarFunction::new(
            "add".to_string(),
            vec![LogicalType::Double, LogicalType::Double],
            LogicalType::Double,
            dummy_function,
        ));

        let (matched, _) = set
            .bind(&[LogicalType::Integer, LogicalType::Integer])
            .expect("bind integer overload");
        assert_eq!(matched.return_type, LogicalType::Integer);

        let (matched, _) = set
            .bind(&[LogicalType::Double, LogicalType::Double])
            .expect("bind double overload");
        assert_eq!(matched.return_type, LogicalType::Double);
    }

    #[test]
    fn scalar_function_set_bind_varargs() {
        let mut set = ScalarFunctionSet::new("concat".to_string());
        set.add_function(
            ScalarFunction::new(
                "concat".to_string(),
                vec![],
                LogicalType::Varchar,
                dummy_function,
            )
            .with_dispatch(ScalarDispatch::Variadic(dummy_function))
            .with_varargs(LogicalType::Varchar),
        );

        let (_, target_types) = set.bind(&[]).expect("bind empty varargs");
        assert!(target_types.is_empty());

        let (_, target_types) = set
            .bind(&[LogicalType::Varchar, LogicalType::Varchar])
            .expect("bind two varargs");
        assert_eq!(target_types.len(), 2);
    }

    #[test]
    fn array_size_inference_from_string_literal() {
        let mut set = ScalarFunctionSet::new("l2_distance".to_string());
        set.add_function(ScalarFunction::new(
            "l2_distance".to_string(),
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 0),
                LogicalType::Array(Box::new(LogicalType::Float), 0),
            ],
            LogicalType::Double,
            dummy_function,
        ));

        let (_, target_types) = set
            .bind(&[
                LogicalType::Array(Box::new(LogicalType::Float), 3),
                LogicalType::StringLiteral,
            ])
            .expect("specialize string literal against array");
        assert_eq!(
            target_types,
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 3),
                LogicalType::Array(Box::new(LogicalType::Float), 3),
            ]
        );

        let (_, target_types) = set
            .bind(&[
                LogicalType::StringLiteral,
                LogicalType::Array(Box::new(LogicalType::Float), 5),
            ])
            .expect("specialize array against string literal");
        assert_eq!(
            target_types,
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 5),
                LogicalType::Array(Box::new(LogicalType::Float), 5),
            ]
        );

        let (_, target_types) = set
            .bind(&[
                LogicalType::Array(Box::new(LogicalType::Float), 3),
                LogicalType::Array(Box::new(LogicalType::Float), 3),
            ])
            .expect("keep matching arrays specialized");
        assert_eq!(
            target_types,
            vec![
                LogicalType::Array(Box::new(LogicalType::Float), 3),
                LogicalType::Array(Box::new(LogicalType::Float), 3),
            ]
        );
    }

    #[test]
    fn list_child_type_inference_from_generic_signature() {
        let mut set = ScalarFunctionSet::new("array_length".to_string());
        set.add_function(ScalarFunction::new(
            "array_length".to_string(),
            vec![
                LogicalType::List(Box::new(LogicalType::Unknown)),
                LogicalType::Integer,
            ],
            LogicalType::Integer,
            dummy_function,
        ));

        let (_, target_types) = set
            .bind(&[
                LogicalType::List(Box::new(LogicalType::BigInt)),
                LogicalType::Integer,
            ])
            .expect("specialize list child type");

        assert_eq!(
            target_types,
            vec![
                LogicalType::List(Box::new(LogicalType::BigInt)),
                LogicalType::Integer,
            ]
        );
    }
}
