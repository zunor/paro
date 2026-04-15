//! Compiled expression state and reusable node-local scratch.

use std::collections::HashSet;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::FunctionLocalState;

use super::comparison::{ComparisonFn, ComparisonSelectFn};

#[derive(Debug, Clone)]
pub enum EvaluatedValue {
    Borrowed(Vector),
    Scratch(Vector),
}

impl EvaluatedValue {
    pub fn as_vector(&self) -> &Vector {
        match self {
            Self::Borrowed(vector) | Self::Scratch(vector) => vector,
        }
    }

    pub fn write_into(self, result: &mut Vector) {
        match self {
            Self::Borrowed(vector) => {
                *result = vector;
            }
            Self::Scratch(vector) => {
                *result = vector;
                result.make_exclusive();
            }
        }
    }
}

#[derive(Debug, Default)]
pub enum ValueSlot {
    #[default]
    Empty,
    Value(Vector),
}

impl ValueSlot {
    pub fn as_ref(&self) -> Option<&Vector> {
        match self {
            Self::Empty => None,
            Self::Value(vector) => Some(vector),
        }
    }

    pub fn set_value(&mut self, value: Vector) {
        *self = Self::Value(value);
    }

    pub fn evaluated(&self, scratch: bool) -> Option<EvaluatedValue> {
        match self {
            Self::Empty => None,
            Self::Value(vector) => Some(if scratch {
                EvaluatedValue::Scratch(vector.reference())
            } else {
                EvaluatedValue::Borrowed(vector.reference())
            }),
        }
    }

    pub fn prepare_scratch(
        &mut self,
        logical_type: &LogicalType,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> &mut Vector {
        let required_capacity = count.max(1);
        match self {
            Self::Empty => {
                *self = Self::Value(Vector::with_capacity_and_allocator(
                    logical_type.clone(),
                    required_capacity,
                    allocator.clone(),
                ));
            }
            Self::Value(vector) => {
                if vector.logical_type() != logical_type || vector.capacity() < required_capacity {
                    *vector = Vector::with_capacity_and_allocator(
                        logical_type.clone(),
                        required_capacity,
                        allocator.clone(),
                    );
                } else {
                    vector.reset_for_execution(required_capacity, allocator.clone());
                }
            }
        }

        let vector = match self {
            Self::Empty => unreachable!(),
            Self::Value(vector) => vector,
        };
        vector.set_len(count);
        vector
    }
}

#[derive(Debug)]
pub enum CompiledExpressionState {
    Function(ExecuteFunctionState),
    Cast(CastExpressionState),
    Comparison(ComparisonExpressionState),
    Conjunction(ConjunctionExpressionState),
    Case(CaseExpressionState),
    Constant(ConstantExpressionState),
    ColumnRef(ColumnRefExpressionState),
    Operator(OperatorExpressionState),
    Reference(ReferenceExpressionState),
    Subquery(SubqueryExpressionState),
    Window(WindowExpressionState),
}

#[derive(Debug)]
pub struct ExecuteFunctionState {
    pub child_states: Vec<CompiledExpressionState>,
    pub intermediate_types: Vec<LogicalType>,
    pub intermediate_chunk: Option<Chunk>,
    pub local_state: Option<Box<dyn FunctionLocalState>>,
    pub cached_dictionary_input_id: Option<CachedDictionaryInputId>,
    pub cached_dictionary_output: Option<Arc<Vector>>,
    pub result: ValueSlot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedDictionaryInputId {
    pub provenance_id: u64,
    pub unique_len: usize,
}

#[derive(Debug)]
pub struct CastExpressionState {
    pub child: Box<CompiledExpressionState>,
    pub child_result: ValueSlot,
    pub result: ValueSlot,
}

#[derive(Debug)]
pub struct ComparisonExpressionState {
    pub left: Box<CompiledExpressionState>,
    pub right: Box<CompiledExpressionState>,
    pub(crate) compare: ComparisonFn,
    pub(crate) select: Option<ComparisonSelectFn>,
    pub left_result: ValueSlot,
    pub right_result: ValueSlot,
    pub result: ValueSlot,
}

#[derive(Debug)]
pub struct ConjunctionExpressionState {
    pub child_states: Vec<CompiledExpressionState>,
    pub ping: ValueSlot,
    pub pong: ValueSlot,
}

#[derive(Debug)]
pub struct CaseExpressionState {
    pub check: Box<CompiledExpressionState>,
    pub result_if_true: Box<CompiledExpressionState>,
    pub result_if_false: Box<CompiledExpressionState>,
    pub check_result: ValueSlot,
    pub true_result: ValueSlot,
    pub false_result: ValueSlot,
    pub result: ValueSlot,
}

#[derive(Debug)]
pub struct ConstantExpressionState;

#[derive(Debug)]
pub struct ColumnRefExpressionState;

#[derive(Debug)]
pub struct OperatorExpressionState {
    pub child_states: Vec<CompiledExpressionState>,
    pub child_results: Vec<ValueSlot>,
    pub(crate) in_list: Option<PreparedInList>,
    pub result: ValueSlot,
    pub aux: ValueSlot,
    pub(crate) scratch: ValueSlot,
}

#[derive(Debug)]
pub(crate) enum PreparedInList {
    Dynamic,
    SmallConst {
        values: Vec<Value>,
        has_null: bool,
    },
    HashedConst {
        values: HashSet<Value>,
        has_null: bool,
    },
}

#[derive(Debug)]
pub struct ReferenceExpressionState;

#[derive(Debug)]
pub struct SubqueryExpressionState;

#[derive(Debug)]
pub struct WindowExpressionState;
