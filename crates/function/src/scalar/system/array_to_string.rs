//! array_to_string() Function
//!
//! PostgreSQL-compatible function that converts an array/list to a string.
//!
//!
//!
//! ## PostgreSQL Reference
//! `array_to_string(array anyarray, delimiter text [, null_string text]) -> text`
//! Concatenates array elements using the specified delimiter.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{ArrayVector, ArrayView, DataRef, Vector, VectorType, VectorView};

use crate::scalar::executor::varlen::VarcharResultWriter;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

fn read_list_entry(
    entries: &VectorView<'_>,
    child_len: usize,
    row: usize,
) -> Result<(usize, usize)> {
    let DataRef::Ptr(entry_data) = entries.data() else {
        return Err(paro_error::internal(
            "array_to_string does not support sequence-backed LIST entries",
        ));
    };
    let entry_ptr = unsafe { entry_data.add(entries.physical_index(row) * 8) as *const u32 };
    let offset = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
    let length = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };

    if offset.saturating_add(length) > child_len {
        return Err(paro_error::internal(format!(
            "Invalid list entry ({offset}, {length}), child length is {child_len}",
        )));
    }

    Ok((offset, length))
}

fn list_child_vector(vector: &Vector) -> Result<&Vector> {
    match vector.vector_type() {
        VectorType::Dictionary => {
            let child = vector
                .child()
                .ok_or_else(|| paro_error::internal("Dictionary LIST missing child"))?;
            list_child_vector(child)
        }
        VectorType::Flat | VectorType::Constant => vector
            .child()
            .map(|child| child.as_ref())
            .ok_or_else(|| paro_error::internal("List vector missing child")),
        VectorType::Sequence => Err(paro_error::type_mismatch(
            "array_to_string does not support sequence-backed LIST vectors",
        )),
    }
}

fn append_serialized_value(
    out: &mut String,
    first: &mut bool,
    delimiter: &str,
    null_string: Option<&str>,
    value: &Value,
) {
    if value.is_null() {
        if let Some(replacement) = null_string {
            if !*first {
                out.push_str(delimiter);
            }
            out.push_str(replacement);
            *first = false;
        }
        return;
    }

    if !*first {
        out.push_str(delimiter);
    }
    out.push_str(&value_to_text(value));
    *first = false;
}

fn serialize_array_row(
    source: &Vector,
    array: &ArrayView<'_>,
    row: usize,
    delimiter: &str,
    null_string: Option<&str>,
) -> String {
    let child = ArrayVector::get_entry(source);
    let mut out = String::new();
    let mut first = true;

    for offset in 0..array.array_size() {
        let value = if array.child_is_valid(row, offset) {
            let physical_idx = array.physical_child_index(row, offset);
            child.get_value(physical_idx)
        } else {
            Value::Null(child.logical_type().clone())
        };
        append_serialized_value(&mut out, &mut first, delimiter, null_string, &value);
    }

    out
}

fn serialize_list_row(
    entries: &VectorView<'_>,
    child_view: &VectorView<'_>,
    child_vector: &Vector,
    row: usize,
    delimiter: &str,
    null_string: Option<&str>,
) -> Result<String> {
    let (offset, length) = read_list_entry(entries, child_vector.len(), row)?;
    let mut out = String::new();
    let mut first = true;

    for child_idx in offset..offset + length {
        let value = if child_view.is_valid(child_idx) {
            let physical_idx = child_view.physical_index(child_idx);
            child_vector.get_value(physical_idx)
        } else {
            Value::Null(child_vector.logical_type().clone())
        };
        append_serialized_value(&mut out, &mut first, delimiter, null_string, &value);
    }

    Ok(out)
}

fn nested_to_text(values: &[Value]) -> String {
    let mut out = String::new();
    out.push('[');
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value_to_text(value));
    }
    out.push(']');
    out
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Varchar(v) => v.clone(),
        Value::Blob(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::List(values, _) => nested_to_text(values),
        Value::Array(values, _, _) => nested_to_text(values),
        _ => value.to_string(),
    }
}

fn array_to_string_impl_internal(
    input: &Chunk,
    result: &mut Vector,
    null_string_idx: Option<usize>,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);

    let array_vec = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing array/list input column"))?;
    let delimiter_vec = input
        .column(1)
        .ok_or_else(|| paro_error::internal("Missing delimiter column"))?;
    let null_string_vec = if let Some(idx) = null_string_idx {
        Some(
            input
                .column(idx)
                .ok_or_else(|| paro_error::internal("Missing null_string column"))?,
        )
    } else {
        None
    };
    let array_view = matches!(array_vec.logical_type(), LogicalType::Array(_, _))
        .then(|| array_vec.to_array_view(count));
    let list_entries =
        matches!(array_vec.logical_type(), LogicalType::List(_)).then(|| array_vec.to_view(count));
    let list_child = if matches!(array_vec.logical_type(), LogicalType::List(_)) {
        Some(list_child_vector(array_vec)?)
    } else {
        None
    };
    let list_child_view = list_child.map(|child| child.to_view(child.len()));
    let mut writer = VarcharResultWriter::new(result, count);

    for i in 0..count {
        let collection_is_null = if let Some(array) = &array_view {
            !array.is_valid(i)
        } else if let Some(entries) = &list_entries {
            !entries.is_valid(i)
        } else {
            return Err(paro_error::type_mismatch(format!(
                "array_to_string can only be used on arrays or lists, got {}",
                array_vec.logical_type()
            )));
        };

        if collection_is_null || delimiter_vec.is_null(i) {
            writer.set_null(i);
            continue;
        }

        let delimiter = delimiter_vec
            .get_string(i)
            .ok_or_else(|| paro_error::internal("Delimiter must be VARCHAR"))?;
        let null_string = null_string_vec.and_then(|vec| {
            if vec.is_null(i) {
                None
            } else {
                vec.get_string(i)
            }
        });

        let serialized = if let Some(array) = &array_view {
            serialize_array_row(array_vec, array, i, delimiter, null_string)
        } else {
            serialize_list_row(
                list_entries.as_ref().expect("LIST entries view"),
                list_child_view.as_ref().expect("LIST child view"),
                list_child.expect("LIST child vector"),
                i,
                delimiter,
                null_string,
            )?
        };
        writer.write_str(i, &serialized);
    }

    Ok(())
}

fn array_to_string_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    array_to_string_impl_internal(input, result, None)
}

fn array_to_string_with_null_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    array_to_string_impl_internal(input, result, Some(2))
}

/// Get `array_to_string` function set.
pub fn get_array_to_string_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("array_to_string".to_string());

    // array_to_string(ARRAY<ANY>, delimiter)
    set.add_function(ScalarFunction::new(
        "array_to_string".to_string(),
        vec![
            LogicalType::Array(Box::new(LogicalType::Unknown), 0),
            LogicalType::Varchar,
        ],
        LogicalType::Varchar,
        array_to_string_impl,
    ));

    // array_to_string(ARRAY<ANY>, delimiter, null_string)
    set.add_function(ScalarFunction::new(
        "array_to_string".to_string(),
        vec![
            LogicalType::Array(Box::new(LogicalType::Unknown), 0),
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
        LogicalType::Varchar,
        array_to_string_with_null_impl,
    ));

    // array_to_string(LIST<ANY>, delimiter)
    set.add_function(ScalarFunction::new(
        "array_to_string".to_string(),
        vec![
            LogicalType::List(Box::new(LogicalType::Unknown)),
            LogicalType::Varchar,
        ],
        LogicalType::Varchar,
        array_to_string_impl,
    ));

    // array_to_string(LIST<ANY>, delimiter, null_string)
    set.add_function(ScalarFunction::new(
        "array_to_string".to_string(),
        vec![
            LogicalType::List(Box::new(LogicalType::Unknown)),
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
        LogicalType::Varchar,
        array_to_string_with_null_impl,
    ));

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::sync::Arc;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn int_array(values: &[Option<Vec<i32>>], width: usize) -> Vector {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), width);
        let mut vec = Vector::new_array(array_type, values.len());
        vec.set_count(values.len());
        for (idx, row) in values.iter().enumerate() {
            match row {
                Some(items) => {
                    let elems = items
                        .iter()
                        .copied()
                        .map(Value::Integer)
                        .collect::<Vec<_>>();
                    vec.set_value(idx, &Value::Array(elems, LogicalType::Integer, width));
                }
                None => vec.set_null(idx, true),
            }
        }
        vec
    }

    #[test]
    fn test_array_to_string_basic() {
        let array_vec = int_array(&[Some(vec![1, 2, 3])], 3);
        let delimiter_vec = Vector::from_strings(&[","]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("1,2,3"));
    }

    #[test]
    fn test_array_to_string_skips_null_without_null_string() {
        let array_vec = Vector::constant_from_value(
            LogicalType::Array(Box::new(LogicalType::Integer), 3),
            Value::Array(
                vec![
                    Value::Integer(1),
                    Value::Null(LogicalType::Integer),
                    Value::Integer(3),
                ],
                LogicalType::Integer,
                3,
            ),
            1,
        );
        let delimiter_vec = Vector::from_strings(&[","]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("1,3"));
    }

    #[test]
    fn test_array_to_string_with_null_string() {
        let array_vec = Vector::constant_from_value(
            LogicalType::Array(Box::new(LogicalType::Integer), 3),
            Value::Array(
                vec![
                    Value::Integer(1),
                    Value::Null(LogicalType::Integer),
                    Value::Integer(3),
                ],
                LogicalType::Integer,
                3,
            ),
            1,
        );
        let delimiter_vec = Vector::from_strings(&[","]);
        let null_string_vec = Vector::from_strings(&["*"]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec, null_string_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_with_null_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("1,*,3"));
    }

    #[test]
    fn test_array_to_string_string_elements_not_quoted() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Varchar), 3);
        let array_vec = Vector::constant_from_value(
            array_type,
            Value::Array(
                vec![
                    Value::Varchar("alpha".to_string()),
                    Value::Varchar("beta".to_string()),
                    Value::Varchar("gamma".to_string()),
                ],
                LogicalType::Varchar,
                3,
            ),
            1,
        );
        let delimiter_vec = Vector::from_strings(&["|"]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("alpha|beta|gamma"));
    }

    #[test]
    fn test_array_to_string_list_input() {
        let mut list_vec =
            Vector::with_capacity(LogicalType::List(Box::new(LogicalType::Integer)), 2);
        list_vec.set_count(2);
        list_vec.set_child(Arc::new(Vector::from_i32(&[1, 2, 3, 4, 5])));
        unsafe {
            let entries = list_vec.flat_data_mut::<u32>();
            *entries.add(0) = 0;
            *entries.add(1) = 2;
            *entries.add(2) = 2;
            *entries.add(3) = 3;
        }

        let delimiter_vec = Vector::from_strings(&["|", "|"]);
        let chunk = Chunk::from_vectors(vec![list_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("1|2"));
        assert_eq!(result.get_string(1), Some("3|4|5"));
    }

    #[test]
    fn test_array_to_string_null_propagation() {
        let mut array_vec = int_array(&[Some(vec![1, 2, 3])], 3);
        array_vec.set_null(0, true);
        let delimiter_vec = Vector::from_strings(&[","]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_array_to_string_null_delimiter_produces_null() {
        let array_vec = int_array(&[Some(vec![1, 2, 3])], 3);
        let mut delimiter_vec = Vector::from_strings(&[","]);
        delimiter_vec.set_null(0, true);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_array_to_string_empty_array() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 0);
        let array_vec = Vector::constant_from_value(
            array_type,
            Value::Array(vec![], LogicalType::Integer, 0),
            1,
        );
        let delimiter_vec = Vector::from_strings(&[","]);
        let chunk = Chunk::from_vectors(vec![array_vec, delimiter_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        array_to_string_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some(""));
    }

    #[test]
    fn test_array_to_string_function_set_signatures() {
        let set = get_array_to_string_functions();
        assert_eq!(set.name, "array_to_string");
        assert_eq!(set.functions.len(), 4);
        assert!(set
            .functions
            .iter()
            .all(|func| func.arguments.len() == 2 || func.arguments.len() == 3));
        assert!(set
            .functions
            .iter()
            .all(|func| !matches!(func.arguments[0], LogicalType::Varchar)));
    }
}
