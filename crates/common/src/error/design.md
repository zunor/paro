# Paro Error System Refactoring Design

## 1. 问题分析

### 1.1 当前问题

当前 paro 的错误返回给 psql 客户端时信息冗余，例如：

```
ERROR:  Error at statement 0: Parser Error: Internal Error: Replenish buffer timeout: too many iterations
ERROR:  Internal Error: Replenish buffer timeout: too many iterations
```

问题：
1. **错误信息重复**: 同一条错误消息被显示两次
2. **信息层次混乱**: `Error at statement 0:` + `Parser Error:` + `Internal Error:` 前缀叠加
3. **缺少 SQLSTATE**: 没有正确的 PostgreSQL SQLSTATE 错误码
4. **缺少结构化字段**: 没有 detail、hint、context 等辅助信息

### 1.2 PostgreSQL 错误系统分析

PostgreSQL 的错误系统有以下核心设计:

#### ErrorData 结构 (elog.h)
```c
typedef struct ErrorData {
    int         elevel;          // 错误级别
    int         sqlerrcode;      // SQLSTATE 错误码
    char       *message;         // 主要错误消息
    char       *detail;          // 详细说明
    char       *hint;            // 修复建议
    ...
} ErrorData;
```

## 2. 设计目标

1. **简洁的用户端呈现**: 错误消息只显示一次，无冗余前缀
2. **符合 PostgreSQL 协议**: 完整支持 ErrorResponse 的所有字段
3. **标准 SQLSTATE**: 使用 PostgreSQL 兼容的 SQLSTATE 错误码
4. **极简 API**: `paro_error::syntax(msg)` 直接使用，无需知道分类
5. **外部错误兼容**: 支持包装 Databend Parser 等外部库的错误
6. **扁平目录结构**: 减少嵌套层级
7. **错误判断 API**: 支持精确匹配、类别匹配和语义谓词判断

## 3. 目录结构

```
crates/common/src/error/
├── mod.rs                 // 模块入口 + 扁平化 API 导出
│
│   # 核心类型 (直接放在 error/ 下，不使用 core/ 子目录)
├── severity.rs            // Severity 枚举
├── sqlstate.rs            // SqlState 类型 + 错误判断方法
├── error_class.rs         // ErrorClass 枚举 (用于 match 表达式)
├── error_data.rs          // ErrorData 结构体
├── paro_error.rs          // ParoError 包装类型 + 错误判断 API
│
│   # SQLSTATE 常量
├── codes/
│   ├── mod.rs
│   ├── success.rs         // 00 - Successful Completion
│   ├── connection.rs      // 08 - Connection Exception
│   ├── feature.rs         // 0A - Feature Not Supported
│   ├── data.rs            // 22 - Data Exception
│   ├── constraint.rs      // 23 - Integrity Constraint Violation
│   ├── transaction.rs     // 25 - Invalid Transaction State
│   ├── syntax.rs          // 42 - Syntax Error or Access Rule Violation
│   ├── resource.rs        // 53 - Insufficient Resources
│   ├── operator.rs        // 57 - Operator Intervention
│   ├── system.rs          // 58 - System Error
│   └── internal.rs        // XX - Internal Error
│
│   # 便捷构造器 (使用 make_ 前缀)
├── make_syntax.rs         // 语法相关错误构造器
├── make_catalog.rs        // Catalog 对象相关错误构造器
├── make_data.rs           // 数据相关错误构造器
├── make_constraint.rs     // 约束相关错误构造器
├── make_transaction.rs    // 事务相关错误构造器
├── make_system.rs         // 系统相关错误构造器
└── make_internal.rs       // 内部错误构造器
```

## 4. 核心类型

### 4.1 Severity

```rust
// severity.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Debug,
    Log,
    Info,
    Notice,
    Warning,
    Error,
    Fatal,
    Panic,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Log => "LOG",
            Self::Info => "INFO",
            Self::Notice => "NOTICE",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
        }
    }

    pub fn aborts_transaction(&self) -> bool {
        matches!(self, Self::Error | Self::Fatal | Self::Panic)
    }
}
```

### 4.2 SqlState

```rust
// sqlstate.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlState([u8; 5]);

impl SqlState {
    pub const fn new(code: [u8; 5]) -> Self {
        Self(code)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("XX000")
    }

    pub fn class(&self) -> &str {
        std::str::from_utf8(&self.0[0..2]).unwrap_or("XX")
    }
}

impl std::fmt::Display for SqlState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
```

### 4.3 ErrorData

```rust
// error_data.rs

use crate::error::{Severity, SqlState};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct ErrorData {
    pub severity: Severity,
    pub sqlstate: SqlState,
    pub message: Cow<'static, str>,

    pub detail: Option<Cow<'static, str>>,
    pub hint: Option<Cow<'static, str>>,
    pub context: Option<String>,

    pub schema_name: Option<Cow<'static, str>>,
    pub table_name: Option<Cow<'static, str>>,
    pub column_name: Option<Cow<'static, str>>,
    pub datatype_name: Option<Cow<'static, str>>,
    pub constraint_name: Option<Cow<'static, str>>,

    pub position: Option<u32>,
}

impl ErrorData {
    pub fn new(
        severity: Severity,
        sqlstate: SqlState,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            severity,
            sqlstate,
            message: message.into(),
            detail: None,
            hint: None,
            context: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            position: None,
        }
    }

    // Builder 方法
    pub fn detail(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.detail = Some(v.into()); self
    }
    pub fn hint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.hint = Some(v.into()); self
    }
    pub fn schema(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.schema_name = Some(v.into()); self
    }
    pub fn table(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.table_name = Some(v.into()); self
    }
    pub fn column(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.column_name = Some(v.into()); self
    }
    pub fn constraint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.constraint_name = Some(v.into()); self
    }
    pub fn position(mut self, pos: u32) -> Self {
        self.position = Some(pos); self
    }
}

impl std::fmt::Display for ErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ErrorData {}
```

### 4.4 ParoError

```rust
// paro_error.rs

use crate::error::ErrorData;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ParoError(pub ErrorData);

impl ParoError {
    pub fn new(data: ErrorData) -> Self {
        Self(data)
    }

    pub fn data(&self) -> &ErrorData {
        &self.0
    }

    pub fn sqlstate(&self) -> crate::error::SqlState {
        self.0.sqlstate
    }

    pub fn message(&self) -> &str {
        &self.0.message
    }

    // Builder 代理方法 - 链式调用
    pub fn detail(mut self, v: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.0.detail = Some(v.into()); self
    }
    pub fn hint(mut self, v: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.0.hint = Some(v.into()); self
    }
    pub fn schema(mut self, v: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.0.schema_name = Some(v.into()); self
    }
    pub fn table(mut self, v: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.0.table_name = Some(v.into()); self
    }
    pub fn column(mut self, v: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.0.column_name = Some(v.into()); self
    }
    pub fn position(mut self, pos: u32) -> Self {
        self.0.position = Some(pos); self
    }
}

impl From<ErrorData> for ParoError {
    fn from(data: ErrorData) -> Self {
        Self(data)
    }
}
```

## 5. SQLSTATE 常量

```rust
// codes/syntax.rs - 语法和访问规则错误 (Class 42)

use crate::error::SqlState;

pub const SYNTAX_ERROR: SqlState = SqlState::new(*b"42601");
pub const INSUFFICIENT_PRIVILEGE: SqlState = SqlState::new(*b"42501");

pub const UNDEFINED_COLUMN: SqlState = SqlState::new(*b"42703");
pub const UNDEFINED_FUNCTION: SqlState = SqlState::new(*b"42883");
pub const UNDEFINED_TABLE: SqlState = SqlState::new(*b"42P01");
pub const UNDEFINED_SCHEMA: SqlState = SqlState::new(*b"3F000");

pub const DUPLICATE_TABLE: SqlState = SqlState::new(*b"42P07");
pub const DUPLICATE_COLUMN: SqlState = SqlState::new(*b"42701");
pub const DUPLICATE_SCHEMA: SqlState = SqlState::new(*b"42P06");

pub const AMBIGUOUS_COLUMN: SqlState = SqlState::new(*b"42702");
pub const AMBIGUOUS_FUNCTION: SqlState = SqlState::new(*b"42725");

pub const DATATYPE_MISMATCH: SqlState = SqlState::new(*b"42804");
pub const GROUPING_ERROR: SqlState = SqlState::new(*b"42803");
```

```rust
// codes/data.rs - 数据异常 (Class 22)

use crate::error::SqlState;

pub const DIVISION_BY_ZERO: SqlState = SqlState::new(*b"22012");
pub const NUMERIC_VALUE_OUT_OF_RANGE: SqlState = SqlState::new(*b"22003");
pub const INVALID_DATETIME_FORMAT: SqlState = SqlState::new(*b"22007");
pub const INVALID_TEXT_REPRESENTATION: SqlState = SqlState::new(*b"22P02");
pub const NULL_VALUE_NOT_ALLOWED: SqlState = SqlState::new(*b"22004");
```

```rust
// codes/feature.rs - 功能不支持 (Class 0A)

use crate::error::SqlState;

pub const FEATURE_NOT_SUPPORTED: SqlState = SqlState::new(*b"0A000");
```

```rust
// codes/internal.rs - 内部错误 (Class XX)

use crate::error::SqlState;

pub const INTERNAL_ERROR: SqlState = SqlState::new(*b"XX000");
pub const DATA_CORRUPTED: SqlState = SqlState::new(*b"XX001");
```

## 6. 便捷构造器 (使用 make_ 前缀)

### 6.1 make_syntax.rs - 语法相关

```rust
// make_syntax.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 语法错误
pub fn syntax(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::SYNTAX_ERROR,
        message.into(),
    ))
}

/// 语法错误（带位置）
pub fn syntax_at(message: impl Into<String>, position: u32) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::syntax::SYNTAX_ERROR, message.into())
            .position(position)
    )
}

/// 从外部 Parser 错误创建（如 Databend Parser）
pub fn from_parser(message: impl Into<String>) -> ParoError {
    syntax(message)
}

/// 从外部 Parser 错误创建（带位置）
pub fn from_parser_at(message: impl Into<String>, position: u32) -> ParoError {
    syntax_at(message, position)
}

/// 功能未实现
pub fn not_implemented(feature: impl Into<String>) -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::feature::FEATURE_NOT_SUPPORTED,
            format!("{} is not yet implemented", feature.into()),
        )
        .hint("This feature may be added in a future release.")
    )
}

/// 功能不支持
pub fn not_supported(feature: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::feature::FEATURE_NOT_SUPPORTED,
        format!("{} is not supported", feature.into()),
    ))
}

/// 类型不匹配
pub fn type_mismatch(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::DATATYPE_MISMATCH,
        message.into(),
    ))
}

/// 分组错误
pub fn grouping_error(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::GROUPING_ERROR,
        message.into(),
    ))
}
```

### 6.2 make_catalog.rs - Catalog 对象相关

```rust
// make_catalog.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 表不存在
pub fn table_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_TABLE,
            format!("relation \"{}\" does not exist", name),
        )
        .table(name.to_string())
    )
}

/// 列不存在
pub fn column_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_COLUMN,
            format!("column \"{}\" does not exist", name),
        )
        .column(name.to_string())
    )
}

/// 函数不存在
pub fn function_not_found(signature: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::UNDEFINED_FUNCTION,
        format!("function {} does not exist", signature.into()),
    ))
}

/// Schema 不存在
pub fn schema_not_found(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::UNDEFINED_SCHEMA,
            format!("schema \"{}\" does not exist", name),
        )
        .schema(name.to_string())
    )
}

/// 表已存在
pub fn table_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_TABLE,
            format!("relation \"{}\" already exists", name),
        )
        .table(name.to_string())
    )
}

/// 列已存在
pub fn column_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_COLUMN,
            format!("column \"{}\" already exists", name),
        )
        .column(name.to_string())
    )
}

/// Schema 已存在
pub fn schema_exists(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::DUPLICATE_SCHEMA,
            format!("schema \"{}\" already exists", name),
        )
        .schema(name.to_string())
    )
}

/// 列引用歧义
pub fn ambiguous_column(name: impl AsRef<str>) -> ParoError {
    let name = name.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::syntax::AMBIGUOUS_COLUMN,
            format!("column reference \"{}\" is ambiguous", name),
        )
        .column(name.to_string())
    )
}
```

### 6.3 make_data.rs - 数据相关

```rust
// make_data.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 除零错误
pub fn division_by_zero() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::DIVISION_BY_ZERO,
        "division by zero",
    ))
}

/// 数值超出范围
pub fn out_of_range(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NUMERIC_VALUE_OUT_OF_RANGE,
        message.into(),
    ))
}

/// 整数溢出
pub fn overflow(datatype: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{} out of range", datatype.as_ref()),
    ))
}

/// 无效值
pub fn invalid_value(datatype: impl AsRef<str>, value: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {}: \"{}\"", datatype.as_ref(), value.as_ref()),
    ))
}

/// NULL 不允许
pub fn null_not_allowed(context: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::NULL_VALUE_NOT_ALLOWED,
        context.into(),
    ))
}

/// 类型转换失败
pub fn cannot_cast(from: impl AsRef<str>, to: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::data::INVALID_TEXT_REPRESENTATION,
        format!("cannot cast type {} to {}", from.as_ref(), to.as_ref()),
    ))
}
```

### 6.4 make_constraint.rs - 约束相关

```rust
// make_constraint.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 唯一约束违反
pub fn unique_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::UNIQUE_VIOLATION,
            format!("duplicate key value violates unique constraint \"{}\"", name),
        )
        .constraint(name.to_string())
    )
}

/// 非空约束违反
pub fn not_null_violation(column: impl AsRef<str>) -> ParoError {
    let name = column.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::NOT_NULL_VIOLATION,
            format!("null value in column \"{}\" violates not-null constraint", name),
        )
        .column(name.to_string())
    )
}

/// 外键约束违反
pub fn foreign_key_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::FOREIGN_KEY_VIOLATION,
            format!("violates foreign key constraint \"{}\"", name),
        )
        .constraint(name.to_string())
    )
}

/// CHECK 约束违反
pub fn check_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::CHECK_VIOLATION,
            format!("new row violates check constraint \"{}\"", name),
        )
        .constraint(name.to_string())
    )
}
```

### 6.5 make_transaction.rs - 事务相关

```rust
// make_transaction.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 事务已失败
pub fn transaction_aborted() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::IN_FAILED_SQL_TRANSACTION,
        "current transaction is aborted, commands ignored until end of transaction block",
    ))
}

/// 没有活动事务
pub fn no_transaction() -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::transaction::NO_ACTIVE_SQL_TRANSACTION,
            "there is no transaction in progress",
        )
        .hint("Use BEGIN to start a transaction.")
    )
}

/// 已有活动事务
pub fn transaction_active() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::transaction::ACTIVE_SQL_TRANSACTION,
        "there is already a transaction in progress",
    ))
}
```

### 6.6 make_system.rs - 系统相关

```rust
// make_system.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// IO 错误
pub fn io(err: std::io::Error) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::system::IO_ERROR, err.to_string())
            .detail(format!("System error: {:?}", err.kind()))
    )
}

/// 内存不足
pub fn out_of_memory() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::resource::OUT_OF_MEMORY,
        "out of memory",
    ))
}

/// 查询取消
pub fn query_canceled() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::operator::QUERY_CANCELED,
        "canceling statement due to user request",
    ))
}
```

### 6.7 make_internal.rs - 内部错误

```rust
// make_internal.rs

use crate::error::{ErrorData, ParoError, Severity};
use crate::error::codes;

/// 内部错误
pub fn internal(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::INTERNAL_ERROR,
        message.into(),
    ))
}

/// 内部错误（带详情）
pub fn internal_detail(message: impl Into<String>, detail: impl Into<String>) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::internal::INTERNAL_ERROR, message.into())
            .detail(detail.into())
    )
}

/// 从任意 std::error::Error 创建内部错误
pub fn from_std<E: std::error::Error>(err: E) -> ParoError {
    internal(err.to_string())
}

/// 数据损坏
pub fn data_corrupted(message: impl Into<String>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::DATA_CORRUPTED,
        message.into(),
    ))
}
```

## 7. 模块入口 - 扁平化导出

```rust
// mod.rs

//! Paro Error System
//!
//! # Quick Start
//!
//! ```rust
//! use paro_common::error;
//!
//! // 直接调用
//! let err = paro_error::syntax("unexpected token");
//! let err = paro_error::table_not_found("users");
//! let err = paro_error::not_implemented("LATERAL JOIN");
//!
//! // 链式添加信息
//! let err = paro_error::table_not_found("users")
//!     .schema("public")
//!     .hint("Check if the table exists.");
//!
//! // 包装 Databend Parser 错误
//! let err = paro_error::from_parser(databend_err.to_string());
//! ```

// 核心类型
mod severity;
mod sqlstate;
mod error_data;
mod error_type;

// SQLSTATE 常量
pub mod codes;

// 便捷构造器
mod make_syntax;
mod make_catalog;
mod make_data;
mod make_constraint;
mod make_transaction;
mod make_system;
mod make_internal;

// =========================================================
// 导出核心类型
// =========================================================
pub use severity::Severity;
pub use sqlstate::SqlState;
pub use error_data::ErrorData;
pub use error_type::ParoError;

// =========================================================
// 扁平化 API - 从各 make_*.rs 导出
// =========================================================

// 语法相关 (make_syntax.rs)
pub use make_syntax::syntax;
pub use make_syntax::syntax_at;
pub use make_syntax::from_parser;
pub use make_syntax::from_parser_at;
pub use make_syntax::not_implemented;
pub use make_syntax::not_supported;
pub use make_syntax::type_mismatch;
pub use make_syntax::grouping_error;

// Catalog 对象相关 (make_catalog.rs)
pub use make_catalog::table_not_found;
pub use make_catalog::column_not_found;
pub use make_catalog::function_not_found;
pub use make_catalog::schema_not_found;
pub use make_catalog::table_exists;
pub use make_catalog::column_exists;
pub use make_catalog::schema_exists;
pub use make_catalog::ambiguous_column;

// 数据相关 (make_data.rs)
pub use make_data::division_by_zero;
pub use make_data::out_of_range;
pub use make_data::overflow;
pub use make_data::invalid_value;
pub use make_data::null_not_allowed;
pub use make_data::cannot_cast;

// 约束相关 (make_constraint.rs)
pub use make_constraint::unique_violation;
pub use make_constraint::not_null_violation;
pub use make_constraint::foreign_key_violation;
pub use make_constraint::check_violation;

// 事务相关 (make_transaction.rs)
pub use make_transaction::transaction_aborted;
pub use make_transaction::no_transaction;
pub use make_transaction::transaction_active;

// 系统相关 (make_system.rs)
pub use make_system::io;
pub use make_system::out_of_memory;
pub use make_system::query_canceled;

// 内部错误 (make_internal.rs)
pub use make_internal::internal;
pub use make_internal::internal_detail;
pub use make_internal::from_std;
pub use make_internal::data_corrupted;

/// Result 类型别名
pub type Result<T> = std::result::Result<T, ParoError>;
```

## 8. 使用示例

### 8.1 基本使用

```rust
use paro_common::error;
use paro_common::error::Result;

fn find_table(name: &str) -> Result<Table> {
    Err(paro_error::table_not_found(name))
}

fn parse_number(s: &str) -> Result<i64> {
    s.parse().map_err(|_| paro_error::invalid_value("integer", s))
}
```

### 8.2 链式调用

```rust
use paro_common::error;

fn find_column(table: &str, column: &str) -> Result<Column> {
    Err(paro_error::column_not_found(column)
        .table(table)
        .schema("public")
        .hint("Check column name spelling."))
}
```

### 8.3 包装 Databend Parser 错误

```rust
use paro_common::error;
use databend_common_ast::parser::parse_expr;

fn parse(sql: &str) -> paro_error::Result<Expr> {
    parse_expr(sql).map_err(|e| paro_error::from_parser(e.to_string()))
}
```

## 9. API 速查表

| 函数名 | 用途 | 所在文件 |
|--------|------|----------|
| `syntax(msg)` | 语法错误 | make_syntax.rs |
| `from_parser(msg)` | 包装 Parser 错误 | make_syntax.rs |
| `not_implemented(feat)` | 未实现 | make_syntax.rs |
| `table_not_found(name)` | 表不存在 | make_catalog.rs |
| `column_not_found(name)` | 列不存在 | make_catalog.rs |
| `table_exists(name)` | 表已存在 | make_catalog.rs |
| `ambiguous_column(name)` | 列歧义 | make_catalog.rs |
| `division_by_zero()` | 除零 | make_data.rs |
| `out_of_range(msg)` | 数值越界 | make_data.rs |
| `invalid_value(type, val)` | 无效值 | make_data.rs |
| `unique_violation(name)` | 唯一约束 | make_constraint.rs |
| `not_null_violation(col)` | 非空约束 | make_constraint.rs |
| `transaction_aborted()` | 事务已失败 | make_transaction.rs |
| `io(err)` | IO 错误 | make_system.rs |
| `internal(msg)` | 内部错误 | make_internal.rs |
| `from_std(err)` | 包装标准错误 | make_internal.rs |

## 10. 错误判断 API

遵循 PostgreSQL 的 SQLSTATE 错误分类机制，提供三种错误判断方式：

### 10.1 ErrorClass 枚举

```rust
// error_class.rs

/// 错误类别（基于 SQLSTATE 前两位）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    Success,            // 00 - Successful Completion
    Warning,            // 01 - Warning
    NoData,             // 02 - No Data
    Connection,         // 08 - Connection Exception
    FeatureNotSupported,// 0A - Feature Not Supported
    Data,               // 22 - Data Exception
    Constraint,         // 23 - Integrity Constraint Violation
    Transaction,        // 25 - Invalid Transaction State
    TransactionRollback,// 40 - Transaction Rollback
    Syntax,             // 42 - Syntax Error or Access Rule Violation
    Resource,           // 53 - Insufficient Resources
    Operator,           // 57 - Operator Intervention
    System,             // 58 - System Error
    Internal,           // XX - Internal Error
    Other,              // Unknown
}
```

### 10.2 SqlState 方法

```rust
impl SqlState {
    /// 精确匹配
    pub fn is(&self, other: SqlState) -> bool;
    
    /// 获取错误类别枚举
    pub fn error_class(&self) -> ErrorClass;
    
    /// 类别谓词
    pub fn is_syntax_class(&self) -> bool;
    pub fn is_data_exception(&self) -> bool;
    pub fn is_constraint_violation(&self) -> bool;
    pub fn is_transaction_error(&self) -> bool;
    pub fn is_internal_error(&self) -> bool;
    // ... 更多
}
```

### 10.3 ParoError 便捷方法

```rust
impl ParoError {
    // === 精确匹配 ===
    pub fn is(&self, code: SqlState) -> bool;
    
    // === 类别匹配 ===
    pub fn error_class(&self) -> ErrorClass;
    pub fn is_class(&self, class: &str) -> bool;
    
    // === 类别谓词 ===
    pub fn is_syntax_error(&self) -> bool;
    pub fn is_data_error(&self) -> bool;
    pub fn is_constraint_error(&self) -> bool;
    pub fn is_transaction_error(&self) -> bool;
    pub fn is_internal_error(&self) -> bool;
    pub fn is_system_error(&self) -> bool;
    pub fn is_connection_error(&self) -> bool;
    
    // === 语义谓词 ===
    pub fn is_retryable(&self) -> bool;      // 序列化失败、死锁
    pub fn is_query_canceled(&self) -> bool; // 查询取消
    pub fn is_undefined_object(&self) -> bool; // 对象不存在
    pub fn is_duplicate_object(&self) -> bool; // 对象已存在
}
```

### 10.4 使用示例

```rust
use paro_common::error::{self, codes, ErrorClass};

fn handle_error(err: &error_type::ParoError) {
    // 方式 1: 精确匹配 SQLSTATE
    if err.is(codes::syntax::UNDEFINED_TABLE) {
        println!("Table not found!");
        return;
    }
    
    // 方式 2: 使用 Rust match 表达式匹配类别
    match err.error_class() {
        ErrorClass::Syntax => handle_syntax_error(err),
        ErrorClass::Constraint => handle_constraint_error(err),
        ErrorClass::Internal => panic!("Internal error: {}", err),
        _ => log::error!("Unhandled: {}", err),
    }
    
    // 方式 3: 使用语义谓词
    if err.is_retryable() {
        retry_transaction();
    }
    
    // 方式 4: 类别字符串匹配
    if err.is_class("42") {
        println!("Syntax/access error");
    }
}
```

## 11. 宏定义

```rust
// 在 paro_common/src/lib.rs 中

/// 返回内部错误
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::paro_error::internal(format!($($arg)*)))
    };
}

/// 条件检查
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err);
        }
    };
}
```

## 11. 参考

- [PostgreSQL elog.h](https://github.com/postgres/postgres/blob/master/src/include/utils/elog.h)
- [PostgreSQL errcodes.txt](https://github.com/postgres/postgres/blob/master/src/backend/utils/errcodes.txt)
