// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Read, Write};

use crate::cast_rules::CastRules;

use super::PhysicalType;

/// Logical SQL types supported by Paro.
///
/// These represent the high-level SQL types that users interact with.
/// Each logical type maps to a physical representation in Vector.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum LogicalType {
    // ========== Primitive Types ==========
    /// Boolean type (true/false)
    Boolean,

    // --- Signed Integers ---
    /// 8-bit signed integer
    TinyInt,
    /// 16-bit signed integer
    SmallInt,
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    BigInt,
    /// 128-bit signed integer
    HugeInt,

    // --- Unsigned Integers ---
    /// 8-bit unsigned integer
    UTinyInt,
    /// 16-bit unsigned integer
    USmallInt,
    /// 32-bit unsigned integer
    UInteger,
    /// 64-bit unsigned integer
    UBigInt,
    /// 128-bit unsigned integer
    UHugeInt,

    // --- Floating Point ---
    /// 32-bit floating point
    Float,
    /// 64-bit floating point
    Double,

    /// Fixed-precision decimal
    Decimal { precision: u8, scale: u8 },

    /// Variable-length string with optional collation
    ///
    /// # Example
    /// ```ignore
    /// // Plain VARCHAR
    /// LogicalType::Varchar
    ///
    /// // VARCHAR with collation
    /// LogicalType::VarcharCollation("NOCASE".to_string())
    /// ```
    Varchar,

    /// VARCHAR with explicit collation
    ///
    /// Supported collations:
    /// - "C" or "POSIX": Binary comparison (default)
    /// - "NOCASE": Case-insensitive comparison
    VarcharCollation(String),

    /// Full-text searchable document vector (PostgreSQL-compatible `tsvector`)
    TsVector,

    /// Full-text query expression (PostgreSQL-compatible `tsquery`)
    TsQuery,

    // --- Temporal Types ---
    /// Date (days since epoch)
    Date,
    /// Timestamp (microseconds since epoch)
    Timestamp,
    /// Timestamp with timezone (stored in UTC microseconds)
    TimestampTz,
    /// Time of day
    Time,
    /// Time interval
    Interval,

    /// Binary data
    Blob,
    /// Universally unique identifier (UUID)
    Uuid,
    /// JSON stored as text
    Json,
    /// JSONB stored as binary JSON (currently stored as text)
    Jsonb,

    // ========== Special Types (Binding-time only) ==========
    /// SQL NULL type (represents the NULL constant during binding)
    /// This is different from Unknown - Null has a concrete type meaning
    Null,

    /// Integer literal (e.g., `42` in SQL)
    /// Stores the actual value for range checking and type inference
    /// This type only exists during binding and is resolved to a concrete integer type
    IntegerLiteral(i64),

    /// String literal (e.g., `'hello'` in SQL)
    /// Can be implicitly cast to any type at parse/bind time
    /// This type only exists during binding and is resolved to VARCHAR or target type
    StringLiteral,

    /// Unknown type (for unresolved expressions)
    #[default]
    Unknown,

    // ========== Compound Types ==========
    /// Fixed-size array (e.g., embeddings, vectors)
    ///
    /// # Example
    /// ```ignore
    /// // AI embedding vector with 1536 dimensions
    /// LogicalType::Array(Box::new(LogicalType::Float), 1536)
    ///
    /// // SQL: CREATE TABLE embeddings (vec FLOAT[1536])
    /// ```
    ///
    /// Physical storage: child Vector contains flattened elements
    Array(Box<LogicalType>, usize),

    /// Variable-length list
    ///
    /// # Example
    /// ```ignore
    /// // List of integers: [1, 2, 3, 4]
    /// LogicalType::List(Box::new(LogicalType::Integer))
    ///
    /// // SQL: SELECT [1, 2, 3] or array_agg(x)
    /// ```
    ///
    /// Physical storage: child Vector contains flattened elements,
    /// main buffer contains offsets
    List(Box<LogicalType>),

    /// Struct type with named fields
    ///
    /// # Example
    /// ```ignore
    /// // STRUCT(name VARCHAR, age INTEGER)
    /// LogicalType::Struct(vec![
    ///     ("name".to_string(), LogicalType::Varchar),
    ///     ("age".to_string(), LogicalType::Integer),
    /// ])
    /// ```
    Struct(Vec<(String, LogicalType)>),
}

impl LogicalType {
    /// Returns the type ID for serialization.
    ///
    /// This is used for compact binary serialization in WAL entries.
    pub fn type_id(&self) -> u8 {
        match self {
            LogicalType::Unknown => 0,
            LogicalType::Boolean => 1,
            LogicalType::TinyInt => 2,
            LogicalType::SmallInt => 3,
            LogicalType::Integer => 4,
            LogicalType::BigInt => 5,
            LogicalType::HugeInt => 6,
            LogicalType::Float => 7,
            LogicalType::Double => 8,
            LogicalType::Varchar => 9,
            LogicalType::Date => 10,
            LogicalType::Timestamp => 11,
            LogicalType::Time => 12,
            LogicalType::Interval => 13,
            LogicalType::Blob => 14,
            LogicalType::Null => 15,
            LogicalType::Decimal { .. } => 16,
            LogicalType::Array(_, _) => 17,
            LogicalType::List(_) => 18,
            LogicalType::Struct(_) => 19,
            LogicalType::UTinyInt => 20,
            LogicalType::USmallInt => 21,
            LogicalType::UInteger => 22,
            LogicalType::UBigInt => 23,
            LogicalType::UHugeInt => 24,
            LogicalType::VarcharCollation(_) => 25,
            LogicalType::TsVector => 26,
            LogicalType::TsQuery => 27,
            LogicalType::TimestampTz => 28,
            LogicalType::Uuid => 29,
            LogicalType::Json => 30,
            LogicalType::Jsonb => 31,
            // Literal types should not be serialized
            LogicalType::IntegerLiteral(_) => 255,
            LogicalType::StringLiteral => 254,
        }
    }

    /// Returns the physical representation of this logical type.
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            LogicalType::Boolean => PhysicalType::Bool,
            LogicalType::TinyInt => PhysicalType::Int8,
            LogicalType::SmallInt => PhysicalType::Int16,
            LogicalType::Integer => PhysicalType::Int32,
            LogicalType::BigInt => PhysicalType::Int64,
            LogicalType::HugeInt => PhysicalType::Int128,
            LogicalType::UTinyInt => PhysicalType::UInt8,
            LogicalType::USmallInt => PhysicalType::UInt16,
            LogicalType::UInteger => PhysicalType::UInt32,
            LogicalType::UBigInt => PhysicalType::UInt64,
            LogicalType::UHugeInt => PhysicalType::UInt128,
            LogicalType::Float => PhysicalType::Float,
            LogicalType::Double => PhysicalType::Double,
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => PhysicalType::Varchar,
            LogicalType::Date => PhysicalType::Int32,
            LogicalType::Timestamp | LogicalType::TimestampTz | LogicalType::Time => {
                PhysicalType::Int64
            }
            LogicalType::Interval => PhysicalType::Int128,
            LogicalType::Uuid => PhysicalType::Int128,
            LogicalType::Array(_, _) => PhysicalType::Array,
            LogicalType::List(_) => PhysicalType::List,
            LogicalType::Struct(_) => PhysicalType::Struct,
            LogicalType::Null => PhysicalType::Int8, // Default for NULL
            LogicalType::Decimal { precision, .. } => {
                if *precision <= 18 {
                    PhysicalType::Int64
                } else {
                    PhysicalType::Int128
                }
            }
            LogicalType::IntegerLiteral(_) => PhysicalType::Int64,
            LogicalType::StringLiteral => PhysicalType::Varchar,
            LogicalType::Unknown => PhysicalType::Int8,
            LogicalType::Blob => PhysicalType::Varchar,
        }
    }

    /// Create a LogicalType from a type ID.
    ///
    /// Note: This only works for simple types. Compound types (Array, List, Struct)
    /// and types with parameters (Decimal, VarcharCollation) require full deserialization.
    pub fn from_type_id(type_id: u8) -> Result<Self> {
        match type_id {
            0 => Ok(LogicalType::Unknown),
            1 => Ok(LogicalType::Boolean),
            2 => Ok(LogicalType::TinyInt),
            3 => Ok(LogicalType::SmallInt),
            4 => Ok(LogicalType::Integer),
            5 => Ok(LogicalType::BigInt),
            6 => Ok(LogicalType::HugeInt),
            7 => Ok(LogicalType::Float),
            8 => Ok(LogicalType::Double),
            9 => Ok(LogicalType::Varchar),
            10 => Ok(LogicalType::Date),
            11 => Ok(LogicalType::Timestamp),
            12 => Ok(LogicalType::Time),
            13 => Ok(LogicalType::Interval),
            14 => Ok(LogicalType::Blob),
            15 => Ok(LogicalType::Null),
            20 => Ok(LogicalType::UTinyInt),
            21 => Ok(LogicalType::USmallInt),
            22 => Ok(LogicalType::UInteger),
            23 => Ok(LogicalType::UBigInt),
            24 => Ok(LogicalType::UHugeInt),
            26 => Ok(LogicalType::TsVector),
            27 => Ok(LogicalType::TsQuery),
            28 => Ok(LogicalType::TimestampTz),
            29 => Ok(LogicalType::Uuid),
            30 => Ok(LogicalType::Json),
            31 => Ok(LogicalType::Jsonb),
            // Compound types cannot be created from type_id alone
            16 | 17 | 18 | 19 | 25 => Err(paro_error::internal(format!(
                "Type ID {} requires full deserialization",
                type_id
            ))),
            _ => Err(paro_error::internal(format!(
                "Unknown type ID: {}",
                type_id
            ))),
        }
    }

    /// Alias for type_size for compatibility.
    #[inline]
    pub fn size(&self) -> usize {
        self.type_size()
    }

    /// Returns the physical size of this type in bytes.
    pub fn type_size(&self) -> usize {
        self.physical_type().size()
    }

    /// Returns the PostgreSQL OID for this type.
    ///
    /// This is used in the pgwire protocol to identify column types in
    /// RowDescription messages.
    ///
    /// # See Also
    /// - Constants defined in `pg_oid` module (BOOLOID, INT4OID, etc.)
    pub fn pg_oid(&self) -> u32 {
        // Delegate to the implementation in pg_oid module
        self.to_pg_oid()
    }

    /// Returns true if this is a primitive (non-compound) type.
    pub fn is_primitive(&self) -> bool {
        !matches!(
            self,
            LogicalType::Array(_, _) | LogicalType::List(_) | LogicalType::Struct(_)
        )
    }

    /// Returns true if this is a numeric type.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal { .. }
        )
    }

    /// Returns true if this is an integer type (signed or unsigned).
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
        )
    }

    /// Returns true if this is a signed integer type.
    pub fn is_signed(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
        )
    }

    /// Returns true if this is an unsigned integer type.
    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
        )
    }

    /// Returns true if this is a floating point type.
    pub fn is_floating(&self) -> bool {
        matches!(self, LogicalType::Float | LogicalType::Double)
    }

    /// Returns true if this is an integral type (signed or unsigned integer).
    ///
    /// Used by `get_expression_return_type()` to identify integer constants.
    #[inline]
    pub fn is_integral(&self) -> bool {
        self.is_integer()
    }

    /// Normalize the type by converting literal types to their concrete equivalents.
    ///
    /// Converts:
    /// - `INTEGER_LITERAL(value)` → `INTEGER` or `BIGINT` based on value range
    /// - `STRING_LITERAL` → `VARCHAR`
    ///
    /// This is used before execution to ensure no literal types remain.
    pub fn normalize_type(&self) -> LogicalType {
        match self {
            LogicalType::IntegerLiteral(v) => {
                if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                    LogicalType::Integer
                } else {
                    LogicalType::BigInt
                }
            }
            LogicalType::StringLiteral => LogicalType::Varchar,
            other => other.clone(),
        }
    }

    /// Returns true if this is a temporal type (Date, Time, Timestamp, Interval).
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            LogicalType::Date
                | LogicalType::Time
                | LogicalType::Timestamp
                | LogicalType::TimestampTz
                | LogicalType::Interval
        )
    }

    /// Returns the "maximum" logical type of two types, which is the type the other type
    /// can be implicitly cast to with the lowest cost.
    pub fn max_logical_type(left: &LogicalType, right: &LogicalType) -> LogicalType {
        // Handle StringLiteral first: always resolve to Varchar
        // (StringLiteral == StringLiteral would return StringLiteral otherwise)
        if matches!(left, LogicalType::StringLiteral) && matches!(right, LogicalType::StringLiteral)
        {
            return LogicalType::Varchar;
        }

        // Handle IntegerLiteral: resolve to concrete integer type
        // Two IntegerLiterals with different values should resolve to Integer
        if let (LogicalType::IntegerLiteral(v1), LogicalType::IntegerLiteral(v2)) = (left, right) {
            // Find the smallest type that fits both values
            let max_val = (*v1).max(*v2);
            let min_val = (*v1).min(*v2);
            if min_val >= i32::MIN as i64 && max_val <= i32::MAX as i64 {
                return LogicalType::Integer;
            } else {
                return LogicalType::BigInt;
            }
        }

        // Nested common types must combine their children rather than selecting
        // one whole container by cast cost. Selecting LIST(DECIMAL(2,1)) over
        // ARRAY(DECIMAL(3,2), 2), for example, loses a fractional digit before
        // the value ever reaches its final target type.
        match (left, right) {
            (
                LogicalType::Array(left_child, left_size),
                LogicalType::Array(right_child, right_size),
            ) => {
                let child = Box::new(Self::max_logical_type(left_child, right_child));
                return if left_size == right_size {
                    LogicalType::Array(child, *left_size)
                } else {
                    // Different fixed lengths share a lossless variable-length
                    // representation. This also covers empty array literals.
                    LogicalType::List(child)
                };
            }
            (LogicalType::List(left_child), LogicalType::List(right_child))
            | (LogicalType::List(left_child), LogicalType::Array(right_child, _))
            | (LogicalType::Array(left_child, _), LogicalType::List(right_child)) => {
                return LogicalType::List(Box::new(Self::max_logical_type(
                    left_child,
                    right_child,
                )));
            }
            _ => {}
        }

        if left == right {
            return left.clone();
        }
        if let (
            LogicalType::Decimal {
                precision: left_precision,
                scale: left_scale,
            },
            LogicalType::Decimal {
                precision: right_precision,
                scale: right_scale,
            },
        ) = (left, right)
        {
            let scale = (*left_scale).max(*right_scale);
            let integral = left_precision
                .saturating_sub(*left_scale)
                .max(right_precision.saturating_sub(*right_scale));
            let precision = integral.saturating_add(scale).min(38);
            return LogicalType::Decimal {
                precision,
                scale: scale.min(precision),
            };
        }
        if matches!(left, LogicalType::Unknown) {
            return right.clone();
        }
        if matches!(right, LogicalType::Unknown) {
            return left.clone();
        }
        if matches!(left, LogicalType::Null) {
            return match right {
                LogicalType::StringLiteral => LogicalType::Varchar,
                LogicalType::IntegerLiteral(v) => {
                    if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                        LogicalType::Integer
                    } else {
                        LogicalType::BigInt
                    }
                }
                _ => right.clone(),
            };
        }
        if matches!(right, LogicalType::Null) {
            return match left {
                LogicalType::StringLiteral => LogicalType::Varchar,
                LogicalType::IntegerLiteral(v) => {
                    if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                        LogicalType::Integer
                    } else {
                        LogicalType::BigInt
                    }
                }
                _ => left.clone(),
            };
        }

        // FLOAT and DECIMAL cannot represent one another without narrowing,
        // but both have DOUBLE as their established mixed-numeric domain.
        // Resolve that common supertype explicitly instead of falling through
        // to the non-numeric VARCHAR fallback.
        if matches!(
            (left, right),
            (LogicalType::Float, LogicalType::Decimal { .. })
                | (LogicalType::Decimal { .. }, LogicalType::Float)
        ) {
            return LogicalType::Double;
        }

        // IntegerLiteral vs concrete type: resolve to concrete type
        if let LogicalType::IntegerLiteral(_) = left {
            if matches!(
                right,
                LogicalType::TinyInt
                    | LogicalType::SmallInt
                    | LogicalType::Integer
                    | LogicalType::BigInt
                    | LogicalType::HugeInt
                    | LogicalType::UTinyInt
                    | LogicalType::USmallInt
                    | LogicalType::UInteger
                    | LogicalType::UBigInt
                    | LogicalType::UHugeInt
                    | LogicalType::Float
                    | LogicalType::Double
            ) {
                return right.clone();
            }
        }
        if let LogicalType::IntegerLiteral(_) = right {
            if matches!(
                left,
                LogicalType::TinyInt
                    | LogicalType::SmallInt
                    | LogicalType::Integer
                    | LogicalType::BigInt
                    | LogicalType::HugeInt
                    | LogicalType::UTinyInt
                    | LogicalType::USmallInt
                    | LogicalType::UInteger
                    | LogicalType::UBigInt
                    | LogicalType::UHugeInt
                    | LogicalType::Float
                    | LogicalType::Double
            ) {
                return left.clone();
            }
        }

        // StringLiteral vs anything else: take the normalized type of the other operand
        //
        // Prefer a common type when either side can be widened safely.
        //
        // This allows STRING_LITERAL to adapt to the other operand's type.
        // For example: `date_col = '2024-01-01'` → compare as DATE, not VARCHAR
        if matches!(left, LogicalType::StringLiteral) {
            return right.normalize_type();
        }
        if matches!(right, LogicalType::StringLiteral) {
            return left.normalize_type();
        }

        let left_to_right_cost = CastRules::implicit_cast_cost(left, right);
        let right_to_left_cost = CastRules::implicit_cast_cost(right, left);

        if left_to_right_cost >= 0
            && (right_to_left_cost < 0 || left_to_right_cost <= right_to_left_cost)
        {
            right.clone()
        } else if right_to_left_cost >= 0 {
            left.clone()
        } else {
            // No implicit cast possible, default to VARCHAR or error
            // Fallback to VARCHAR when no better common type is available.
            LogicalType::Varchar
        }
    }

    /// Returns the element type for Array or List types.
    pub fn element_type(&self) -> Option<&LogicalType> {
        match self {
            LogicalType::Array(elem, _) => Some(elem.as_ref()),
            LogicalType::List(elem) => Some(elem.as_ref()),
            _ => None,
        }
    }

    /// Returns the array dimension for Array types.
    pub fn array_dimension(&self) -> Option<usize> {
        match self {
            LogicalType::Array(_, dim) => Some(*dim),
            _ => None,
        }
    }

    /// Creates a new Array type (convenience constructor for embeddings).
    ///
    /// # Example
    /// ```ignore
    /// // FLOAT[1536] for OpenAI embeddings
    /// let embedding_type = LogicalType::embedding(1536);
    /// ```
    pub fn embedding(dimensions: usize) -> Self {
        LogicalType::Array(Box::new(LogicalType::Float), dimensions)
    }

    /// Creates a VARCHAR type with the specified collation.
    ///
    /// # Example
    /// ```ignore
    /// // Case-insensitive VARCHAR
    /// let nocase_varchar = LogicalType::varchar_collation("NOCASE");
    /// ```
    pub fn varchar_collation(collation: impl Into<String>) -> Self {
        LogicalType::VarcharCollation(collation.into())
    }

    /// Returns the collation for VARCHAR types, if any.
    pub fn collation(&self) -> Option<&str> {
        match self {
            LogicalType::VarcharCollation(c) => Some(c.as_str()),
            _ => None,
        }
    }

    /// Returns true if this is a VARCHAR type (with or without collation).
    pub fn is_varchar(&self) -> bool {
        matches!(
            self,
            LogicalType::Varchar | LogicalType::VarcharCollation(_)
        )
    }

    /// Serialize the logical type to a writer
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        self.write_to(w)
    }

    fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        match self {
            LogicalType::Boolean => w.write_all(&[1])?,
            LogicalType::TinyInt => w.write_all(&[2])?,
            LogicalType::SmallInt => w.write_all(&[3])?,
            LogicalType::Integer => w.write_all(&[4])?,
            LogicalType::BigInt => w.write_all(&[5])?,
            LogicalType::HugeInt => w.write_all(&[6])?,
            LogicalType::Float => w.write_all(&[7])?,
            LogicalType::Double => w.write_all(&[8])?,
            LogicalType::Varchar => w.write_all(&[9])?,
            LogicalType::TsVector => w.write_all(&[26])?,
            LogicalType::TsQuery => w.write_all(&[27])?,
            LogicalType::VarcharCollation(collation) => {
                w.write_all(&[25])?; // New tag for VarcharCollation
                let collation_bytes = collation.as_bytes();
                w.write_all(&(collation_bytes.len() as u32).to_le_bytes())?;
                w.write_all(collation_bytes)?;
            }
            LogicalType::Date => w.write_all(&[10])?,
            LogicalType::Timestamp => w.write_all(&[11])?,
            LogicalType::Time => w.write_all(&[12])?,
            LogicalType::Interval => w.write_all(&[13])?,
            LogicalType::Blob => w.write_all(&[14])?,
            LogicalType::Uuid => w.write_all(&[29])?,
            LogicalType::Json => w.write_all(&[30])?,
            LogicalType::Jsonb => w.write_all(&[31])?,
            LogicalType::Null => w.write_all(&[15])?,
            LogicalType::Unknown => w.write_all(&[0])?, // Treat Unknown as 0
            LogicalType::TimestampTz => w.write_all(&[28])?,

            // Unsigned integers (20-24)
            LogicalType::UTinyInt => w.write_all(&[20])?,
            LogicalType::USmallInt => w.write_all(&[21])?,
            LogicalType::UInteger => w.write_all(&[22])?,
            LogicalType::UBigInt => w.write_all(&[23])?,
            LogicalType::UHugeInt => w.write_all(&[24])?,

            // Literal types (should not be serialized - error)
            LogicalType::IntegerLiteral(_) | LogicalType::StringLiteral => {
                return Err(paro_error::internal(
                    "Cannot serialize literal types - they should be resolved during binding",
                ));
            }

            LogicalType::Decimal { precision, scale } => {
                w.write_all(&[16])?;
                w.write_all(&[*precision])?;
                w.write_all(&[*scale])?;
            }

            LogicalType::Array(child, dim) => {
                w.write_all(&[17])?;
                child.write_to(w)?;
                w.write_all(&(*dim as u64).to_le_bytes())?;
            }

            LogicalType::List(child) => {
                w.write_all(&[18])?;
                child.write_to(w)?;
            }

            LogicalType::Struct(fields) => {
                w.write_all(&[19])?;
                w.write_all(&(fields.len() as u32).to_le_bytes())?;
                for (name, typ) in fields {
                    let name_bytes = name.as_bytes();
                    w.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
                    w.write_all(name_bytes)?;
                    Self::serialize(typ, w)?;
                }
            }
        }
        Ok(())
    }

    /// Deserialize a logical type from a reader
    pub fn deserialize<R: Read>(r: &mut R) -> Result<Self> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;

        match tag[0] {
            1 => Ok(LogicalType::Boolean),
            2 => Ok(LogicalType::TinyInt),
            3 => Ok(LogicalType::SmallInt),
            4 => Ok(LogicalType::Integer),
            5 => Ok(LogicalType::BigInt),
            6 => Ok(LogicalType::HugeInt),
            7 => Ok(LogicalType::Float),
            8 => Ok(LogicalType::Double),
            9 => Ok(LogicalType::Varchar),
            26 => Ok(LogicalType::TsVector),
            27 => Ok(LogicalType::TsQuery),
            10 => Ok(LogicalType::Date),
            11 => Ok(LogicalType::Timestamp),
            12 => Ok(LogicalType::Time),
            13 => Ok(LogicalType::Interval),
            14 => Ok(LogicalType::Blob),
            29 => Ok(LogicalType::Uuid),
            30 => Ok(LogicalType::Json),
            31 => Ok(LogicalType::Jsonb),
            15 => Ok(LogicalType::Null),
            0 => Ok(LogicalType::Unknown),
            28 => Ok(LogicalType::TimestampTz),

            // Unsigned integers (20-24)
            20 => Ok(LogicalType::UTinyInt),
            21 => Ok(LogicalType::USmallInt),
            22 => Ok(LogicalType::UInteger),
            23 => Ok(LogicalType::UBigInt),
            24 => Ok(LogicalType::UHugeInt),

            16 => {
                let mut params = [0u8; 2];
                r.read_exact(&mut params)?;
                Ok(LogicalType::Decimal {
                    precision: params[0],
                    scale: params[1],
                })
            }

            17 => {
                let child = Box::new(LogicalType::deserialize(r)?);
                let mut dim_buf = [0u8; 8];
                r.read_exact(&mut dim_buf)?;
                let dim = u64::from_le_bytes(dim_buf) as usize;
                Ok(LogicalType::Array(child, dim))
            }

            18 => {
                let child = Box::new(LogicalType::deserialize(r)?);
                Ok(LogicalType::List(child))
            }

            19 => {
                let mut len_buf = [0u8; 4];
                r.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut fields = Vec::with_capacity(len);
                for _ in 0..len {
                    r.read_exact(&mut len_buf)?;
                    let name_len = u32::from_le_bytes(len_buf) as usize;
                    let mut name_bytes = vec![0u8; name_len];
                    r.read_exact(&mut name_bytes)?;
                    let name = String::from_utf8(name_bytes).map_err(|e| {
                        paro_error::internal(format!("Invalid UTF-8 in struct field name: {}", e))
                    })?;

                    let typ = LogicalType::deserialize(r)?;
                    fields.push((name, typ));
                }
                Ok(LogicalType::Struct(fields))
            }

            25 => {
                // VarcharCollation
                let mut len_buf = [0u8; 4];
                r.read_exact(&mut len_buf)?;
                let collation_len = u32::from_le_bytes(len_buf) as usize;
                let mut collation_bytes = vec![0u8; collation_len];
                r.read_exact(&mut collation_bytes)?;
                let collation = String::from_utf8(collation_bytes).map_err(|e| {
                    paro_error::internal(format!("Invalid UTF-8 in collation name: {}", e))
                })?;
                Ok(LogicalType::VarcharCollation(collation))
            }

            _ => Err(paro_error::internal(format!(
                "Unknown LogicalType tag: {}",
                tag[0]
            ))),
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Boolean => write!(f, "BOOLEAN"),
            LogicalType::TinyInt => write!(f, "TINYINT"),
            LogicalType::SmallInt => write!(f, "SMALLINT"),
            LogicalType::Integer => write!(f, "INTEGER"),
            LogicalType::BigInt => write!(f, "BIGINT"),
            LogicalType::HugeInt => write!(f, "HUGEINT"),
            LogicalType::UTinyInt => write!(f, "UTINYINT"),
            LogicalType::USmallInt => write!(f, "USMALLINT"),
            LogicalType::UInteger => write!(f, "UINTEGER"),
            LogicalType::UBigInt => write!(f, "UBIGINT"),
            LogicalType::UHugeInt => write!(f, "UHUGEINT"),
            LogicalType::Float => write!(f, "FLOAT"),
            LogicalType::Double => write!(f, "DOUBLE"),
            LogicalType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({},{})", precision, scale)
            }
            LogicalType::Varchar => write!(f, "VARCHAR"),
            LogicalType::VarcharCollation(collation) => write!(f, "VARCHAR COLLATE {}", collation),
            LogicalType::TsVector => write!(f, "TSVECTOR"),
            LogicalType::TsQuery => write!(f, "TSQUERY"),
            LogicalType::Date => write!(f, "DATE"),
            LogicalType::Timestamp => write!(f, "TIMESTAMP"),
            LogicalType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            LogicalType::Time => write!(f, "TIME"),
            LogicalType::Interval => write!(f, "INTERVAL"),
            LogicalType::Blob => write!(f, "BLOB"),
            LogicalType::Uuid => write!(f, "UUID"),
            LogicalType::Json => write!(f, "JSON"),
            LogicalType::Jsonb => write!(f, "JSONB"),
            LogicalType::Null => write!(f, "NULL"),
            LogicalType::IntegerLiteral(val) => write!(f, "INTEGER_LITERAL({})", val),
            LogicalType::StringLiteral => write!(f, "STRING_LITERAL"),
            LogicalType::Unknown => write!(f, "UNKNOWN"),
            LogicalType::Array(elem, dim) => write!(f, "{}[{}]", elem, dim),
            LogicalType::List(elem) => write!(f, "{}[]", elem),
            LogicalType::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, typ)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", name, typ)?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::nested_types::{ArrayType, ListType, StructType};

    #[test]
    fn test_embedding_type() {
        let embedding = LogicalType::embedding(1536);
        assert_eq!(embedding.array_dimension(), Some(1536));
        assert_eq!(embedding.element_type(), Some(&LogicalType::Float));
        assert!(!embedding.is_primitive());
    }

    #[test]
    fn test_array_type() {
        let arr = LogicalType::Array(Box::new(LogicalType::Integer), 3);
        assert_eq!(arr.element_type(), Some(&LogicalType::Integer));
        assert_eq!(arr.array_dimension(), Some(3));
    }

    #[test]
    fn test_list_type() {
        let list = LogicalType::List(Box::new(LogicalType::Varchar));
        assert_eq!(list.element_type(), Some(&LogicalType::Varchar));
        assert_eq!(list.array_dimension(), None);
        assert!(!list.is_primitive());
    }

    #[test]
    fn test_struct_type() {
        let struct_type = LogicalType::Struct(vec![
            ("name".to_string(), LogicalType::Varchar),
            ("age".to_string(), LogicalType::Integer),
        ]);
        assert!(!struct_type.is_primitive());
    }

    #[test]
    fn test_is_numeric() {
        assert!(LogicalType::Integer.is_numeric());
        assert!(LogicalType::Float.is_numeric());
        assert!(LogicalType::Double.is_numeric());
        assert!(!LogicalType::Varchar.is_numeric());
        assert!(!LogicalType::Boolean.is_numeric());
    }

    #[test]
    fn test_is_primitive() {
        assert!(LogicalType::Integer.is_primitive());
        assert!(LogicalType::Varchar.is_primitive());
        assert!(!LogicalType::Array(Box::new(LogicalType::Float), 3).is_primitive());
        assert!(!LogicalType::List(Box::new(LogicalType::Integer)).is_primitive());
    }

    #[test]
    fn test_max_logical_type() {
        // Same types
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Integer, &LogicalType::Integer),
            LogicalType::Integer
        );

        // Unknown types
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Unknown, &LogicalType::Integer),
            LogicalType::Integer
        );
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Integer, &LogicalType::Unknown),
            LogicalType::Integer
        );

        // SQL Null
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Null, &LogicalType::Integer),
            LogicalType::Integer
        );

        // Numeric promotion
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Integer, &LogicalType::BigInt),
            LogicalType::BigInt
        );
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Integer, &LogicalType::Double),
            LogicalType::Double
        );

        // Literals
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::IntegerLiteral(1), &LogicalType::Double),
            LogicalType::Double
        );

        // Differently sized arrays share a variable-length list type.
        assert_eq!(
            LogicalType::max_logical_type(
                &LogicalType::Array(Box::new(LogicalType::Integer), 3),
                &LogicalType::Array(Box::new(LogicalType::BigInt), 2),
            ),
            LogicalType::List(Box::new(LogicalType::BigInt))
        );
        assert_eq!(
            LogicalType::max_logical_type(
                &LogicalType::Array(Box::new(LogicalType::Null), 0),
                &LogicalType::Array(Box::new(LogicalType::Boolean), 2),
            ),
            LogicalType::List(Box::new(LogicalType::Boolean))
        );

        // Nested child types are widened recursively, including after an ARRAY
        // has already become a LIST because another row had a different length.
        let decimal_2_1 = LogicalType::Decimal {
            precision: 2,
            scale: 1,
        };
        let decimal_3_2 = LogicalType::Decimal {
            precision: 3,
            scale: 2,
        };
        assert_eq!(
            LogicalType::max_logical_type(
                &LogicalType::Array(Box::new(decimal_2_1.clone()), 2),
                &LogicalType::Array(Box::new(decimal_3_2.clone()), 2),
            ),
            LogicalType::Array(Box::new(decimal_3_2.clone()), 2)
        );
        assert_eq!(
            LogicalType::max_logical_type(
                &LogicalType::List(Box::new(decimal_2_1)),
                &LogicalType::Array(Box::new(decimal_3_2.clone()), 2),
            ),
            LogicalType::List(Box::new(decimal_3_2))
        );
    }

    #[test]
    fn decimal_and_float_use_double_common_type() {
        let decimal = LogicalType::Decimal {
            precision: 2,
            scale: 1,
        };
        assert_eq!(
            LogicalType::max_logical_type(&decimal, &LogicalType::Float),
            LogicalType::Double
        );
        assert_eq!(
            LogicalType::max_logical_type(&LogicalType::Float, &decimal),
            LogicalType::Double
        );
    }

    #[test]
    fn test_physical_type() {
        assert_eq!(LogicalType::Integer.physical_type(), PhysicalType::Int32);
        assert_eq!(LogicalType::BigInt.physical_type(), PhysicalType::Int64);
        assert_eq!(LogicalType::Float.physical_type(), PhysicalType::Float);
        assert_eq!(LogicalType::Double.physical_type(), PhysicalType::Double);
        assert_eq!(LogicalType::Varchar.physical_type(), PhysicalType::Varchar);
        assert_eq!(LogicalType::TsVector.physical_type(), PhysicalType::Varchar);
        assert_eq!(LogicalType::TsQuery.physical_type(), PhysicalType::Varchar);
        assert_eq!(
            LogicalType::Array(Box::new(LogicalType::Float), 3).physical_type(),
            PhysicalType::Array
        );
        assert_eq!(
            LogicalType::List(Box::new(LogicalType::Integer)).physical_type(),
            PhysicalType::List
        );
        assert_eq!(
            LogicalType::Struct(vec![("a".to_string(), LogicalType::Integer)]).physical_type(),
            PhysicalType::Struct
        );
    }

    #[test]
    fn test_type_size() {
        assert_eq!(LogicalType::Integer.type_size(), 4);
        assert_eq!(LogicalType::BigInt.type_size(), 8);
        assert_eq!(LogicalType::Float.type_size(), 4);
        assert_eq!(LogicalType::Double.type_size(), 8);
        assert_eq!(LogicalType::Varchar.type_size(), 16);
        assert_eq!(LogicalType::TsVector.type_size(), 16);
        assert_eq!(LogicalType::TsQuery.type_size(), 16);
        assert_eq!(
            LogicalType::Array(Box::new(LogicalType::Float), 3).type_size(),
            0
        );
        assert_eq!(
            LogicalType::List(Box::new(LogicalType::Integer)).type_size(),
            8
        );
        assert_eq!(
            LogicalType::Struct(vec![("a".to_string(), LogicalType::Integer)]).type_size(),
            0
        );
    }

    #[test]
    fn test_tsvector_tsquery_type_ids() {
        assert_eq!(LogicalType::TsVector.type_id(), 26);
        assert_eq!(LogicalType::TsQuery.type_id(), 27);
        assert_eq!(
            LogicalType::from_type_id(26).unwrap(),
            LogicalType::TsVector
        );
        assert_eq!(LogicalType::from_type_id(27).unwrap(), LogicalType::TsQuery);
    }

    #[test]
    fn test_tsvector_tsquery_serialize_deserialize_and_display() {
        for ty in [LogicalType::TsVector, LogicalType::TsQuery] {
            let mut buf = Vec::new();
            ty.serialize(&mut buf).unwrap();
            let decoded = LogicalType::deserialize(&mut buf.as_slice()).unwrap();
            assert_eq!(decoded, ty);
        }
        assert_eq!(LogicalType::TsVector.to_string(), "TSVECTOR");
        assert_eq!(LogicalType::TsQuery.to_string(), "TSQUERY");
    }

    // ========== ArrayType Helper Tests ==========

    #[test]
    fn test_array_type_max_size() {
        // Verify MAX_ARRAY_SIZE stays aligned with the array helper.
        assert_eq!(ArrayType::MAX_ARRAY_SIZE, 100000);
    }

    #[test]
    fn test_array_type_get_child_type() {
        let arr = LogicalType::Array(Box::new(LogicalType::Float), 1536);
        assert_eq!(ArrayType::get_child_type(&arr), &LogicalType::Float);

        // Nested array
        let nested = LogicalType::Array(
            Box::new(LogicalType::Array(Box::new(LogicalType::Integer), 3)),
            4,
        );
        let inner = ArrayType::get_child_type(&nested);
        assert_eq!(ArrayType::get_child_type(inner), &LogicalType::Integer);
    }

    #[test]
    fn test_array_type_get_size() {
        let arr = LogicalType::Array(Box::new(LogicalType::Float), 1536);
        assert_eq!(ArrayType::get_size(&arr), 1536);

        let small_arr = LogicalType::Array(Box::new(LogicalType::Integer), 3);
        assert_eq!(ArrayType::get_size(&small_arr), 3);
    }

    #[test]
    fn test_array_type_is_any_size() {
        // Any size (size == 0)
        let any_size = LogicalType::Array(Box::new(LogicalType::Float), 0);
        assert!(ArrayType::is_any_size(&any_size));

        // Fixed size
        let fixed_size = LogicalType::Array(Box::new(LogicalType::Float), 1536);
        assert!(!ArrayType::is_any_size(&fixed_size));
    }

    #[test]
    fn test_array_type_convert_to_list() {
        // Simple array to list
        let arr = LogicalType::Array(Box::new(LogicalType::Float), 1536);
        let list = ArrayType::convert_to_list(&arr);
        assert_eq!(list, LogicalType::List(Box::new(LogicalType::Float)));

        // Nested array to nested list
        let nested_arr = LogicalType::Array(
            Box::new(LogicalType::Array(Box::new(LogicalType::Integer), 3)),
            4,
        );
        let nested_list = ArrayType::convert_to_list(&nested_arr);
        assert_eq!(
            nested_list,
            LogicalType::List(Box::new(LogicalType::List(Box::new(LogicalType::Integer))))
        );

        // Struct with array field
        let struct_with_arr = LogicalType::Struct(vec![
            (
                "embedding".to_string(),
                LogicalType::Array(Box::new(LogicalType::Float), 1536),
            ),
            ("name".to_string(), LogicalType::Varchar),
        ]);
        let struct_with_list = ArrayType::convert_to_list(&struct_with_arr);
        assert_eq!(
            struct_with_list,
            LogicalType::Struct(vec![
                (
                    "embedding".to_string(),
                    LogicalType::List(Box::new(LogicalType::Float))
                ),
                ("name".to_string(), LogicalType::Varchar),
            ])
        );

        // Non-array types should be unchanged
        assert_eq!(
            ArrayType::convert_to_list(&LogicalType::Integer),
            LogicalType::Integer
        );
        assert_eq!(
            ArrayType::convert_to_list(&LogicalType::Varchar),
            LogicalType::Varchar
        );
    }

    #[test]
    fn test_array_type_new() {
        let arr = ArrayType::create_array(LogicalType::Float, 1536);
        assert_eq!(arr, LogicalType::Array(Box::new(LogicalType::Float), 1536));
        assert_eq!(ArrayType::get_size(&arr), 1536);
        assert_eq!(ArrayType::get_child_type(&arr), &LogicalType::Float);
    }

    #[test]
    fn test_array_type_any_size_constructor() {
        let arr = ArrayType::any_size(LogicalType::Float);
        assert_eq!(arr, LogicalType::Array(Box::new(LogicalType::Float), 0));
        assert!(ArrayType::is_any_size(&arr));
    }

    #[test]
    #[should_panic(expected = "Array size 100001 exceeds maximum allowed size 100000")]
    fn test_array_type_exceeds_max_size() {
        ArrayType::create_array(LogicalType::Float, ArrayType::MAX_ARRAY_SIZE + 1);
    }

    // ========== ListType Helper Tests ==========

    #[test]
    fn test_list_type_get_child_type() {
        let list = LogicalType::List(Box::new(LogicalType::Integer));
        assert_eq!(ListType::get_child_type(&list), &LogicalType::Integer);
    }

    // ========== StructType Helper Tests ==========

    #[test]
    fn test_struct_type_helpers() {
        let struct_type = LogicalType::Struct(vec![
            ("name".to_string(), LogicalType::Varchar),
            ("age".to_string(), LogicalType::Integer),
            ("score".to_string(), LogicalType::Double),
        ]);

        // get_child_types
        let children = StructType::get_child_types(&struct_type);
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].0, "name");
        assert_eq!(children[0].1, LogicalType::Varchar);

        // get_child_type
        assert_eq!(
            StructType::get_child_type(&struct_type, 0),
            &LogicalType::Varchar
        );
        assert_eq!(
            StructType::get_child_type(&struct_type, 1),
            &LogicalType::Integer
        );
        assert_eq!(
            StructType::get_child_type(&struct_type, 2),
            &LogicalType::Double
        );

        // get_child_name
        assert_eq!(StructType::get_child_name(&struct_type, 0), "name");
        assert_eq!(StructType::get_child_name(&struct_type, 1), "age");
        assert_eq!(StructType::get_child_name(&struct_type, 2), "score");

        // get_child_count
        assert_eq!(StructType::get_child_count(&struct_type), 3);
    }
}
