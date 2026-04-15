// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::write_comma_separated_map;
use crate::ast::write_comma_separated_string_map;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableRef;
use crate::ParseError;
use crate::Result;

/// COPY 方向
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum CopyDirection {
    From,
    To,
}

impl Display for CopyDirection {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CopyDirection::From => write!(f, "FROM"),
            CopyDirection::To => write!(f, "TO"),
        }
    }
}

/// COPY 目标：表或查询
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum CopyTarget {
    Table {
        name: TableRef,
        columns: Option<Vec<Identifier>>,
    },
    Query(Box<Query>),
}

impl Display for CopyTarget {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CopyTarget::Table { name, columns } => {
                write!(f, "{name}")?;
                if let Some(columns) = columns {
                    write!(f, " (")?;
                    write_comma_separated_list(f, columns.iter())?;
                    write!(f, ")")?;
                }
                Ok(())
            }
            CopyTarget::Query(query) => write!(f, "({query})"),
        }
    }
}

/// 文件来源
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum CopySource {
    File(String),
    Stdin,
    Stdout,
    Program(String),
}

impl Display for CopySource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CopySource::File(path) => write!(f, "{}", QuotedString(path, '\'')),
            CopySource::Stdin => write!(f, "STDIN"),
            CopySource::Stdout => write!(f, "STDOUT"),
            CopySource::Program(cmd) => write!(f, "PROGRAM {}", QuotedString(cmd, '\'')),
        }
    }
}

/// COPY 选项值类型
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum CopyOptionValue {
    Boolean(bool),
    String(String),
    Number(u64),
    List(Vec<String>),
    Star,
    Default,
}

impl Display for CopyOptionValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CopyOptionValue::Boolean(value) => write!(f, "{value}"),
            CopyOptionValue::String(value) => write!(f, "{}", QuotedString(value, '\'')),
            CopyOptionValue::Number(value) => write!(f, "{value}"),
            CopyOptionValue::List(items) => {
                write!(f, "(")?;
                write_comma_separated_list(f, items.iter())?;
                write!(f, ")")
            }
            CopyOptionValue::Star => write!(f, "*"),
            CopyOptionValue::Default => write!(f, "DEFAULT"),
        }
    }
}

/// COPY 语句
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CopyStmt {
    pub target: CopyTarget,
    pub direction: CopyDirection,
    pub source: CopySource,
    pub options: Vec<(String, CopyOptionValue)>,
    pub where_clause: Option<Box<Expr>>,
}

impl Display for CopyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "COPY {} {} {}", self.target, self.direction, self.source)?;
        if !self.options.is_empty() {
            write!(f, " WITH (")?;
            for (idx, (key, value)) in self.options.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{} {}", key, value)?;
            }
            write!(f, ")")?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct Connection {
    #[drive(skip)]
    visited_keys: HashSet<String>,
    pub conns: BTreeMap<String, String>,
}

impl Connection {
    pub fn new(conns: BTreeMap<String, String>) -> Self {
        Self {
            visited_keys: HashSet::new(),
            conns,
        }
    }

    pub fn mask(&self) -> Self {
        let mut conns = BTreeMap::new();
        for (k, v) in &self.conns {
            conns.insert(k.to_string(), mask_string(v, 3));
        }
        Self {
            visited_keys: self.visited_keys.clone(),
            conns,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.visited_keys.insert(key.to_string());
        self.conns.get(key)
    }

    pub fn check(&self) -> Result<()> {
        let conn_keys = HashSet::from_iter(self.conns.keys().cloned());
        let diffs: Vec<String> = conn_keys
            .difference(&self.visited_keys)
            .map(|x| x.to_string())
            .collect();

        if !diffs.is_empty() {
            return Err(ParseError::without_span(format!(
                "connection params invalid: expected [{}], got [{}]",
                self.visited_keys
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(","),
                diffs.join(",")
            )));
        }
        Ok(())
    }
}

impl Display for Connection {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if !self.conns.is_empty() {
            write!(f, " CONNECTION = ( ")?;
            write_comma_separated_string_map(f, &self.conns)?;
            write!(f, " )")?;
        }
        Ok(())
    }
}

/// Mask a string by "******", but keep `unmask_len` of suffix.
fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}

/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct UriLocation {
    pub protocol: String,
    pub name: String,
    pub path: String,
    pub connection: Connection,
}

impl UriLocation {
    pub fn new(
        protocol: String,
        name: String,
        path: String,
        conns: BTreeMap<String, String>,
    ) -> Self {
        Self {
            protocol,
            name,
            path,
            connection: Connection::new(conns),
        }
    }

    pub fn from_uri(uri: String, conns: BTreeMap<String, String>) -> Result<Self> {
        // fs location is not a valid url, let's check it in advance.
        if let Some(path) = uri.strip_prefix("fs://") {
            if !path.starts_with('/') {
                return Err(ParseError::without_span(format!(
                    "Invalid uri: {}. fs location must start with 'fs:///'",
                    uri
                )));
            }
            return Ok(UriLocation::new(
                "fs".to_string(),
                "".to_string(),
                path.to_string(),
                BTreeMap::default(),
            ));
        }

        let parsed =
            Url::parse(&uri).map_err(|e| ParseError::without_span(format!("invalid uri {}", e)))?;

        let protocol = parsed.scheme().to_string();

        let name = parsed
            .host_str()
            .map(|hostname| {
                if let Some(port) = parsed.port() {
                    format!("{}:{}", hostname, port)
                } else {
                    hostname.to_string()
                }
            })
            .ok_or_else(|| ParseError::without_span("invalid uri"))?;

        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            percent_decode_str(parsed.path())
                .decode_utf8_lossy()
                .to_string()
        };

        Ok(Self {
            protocol,
            name,
            path,
            connection: Connection::new(conns),
        })
    }

    pub fn mask(&self) -> Self {
        Self {
            connection: self.connection.mask(),
            ..self.clone()
        }
    }
}

impl Display for UriLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "'{}://{}{}'", self.protocol, self.name, self.path)?;
        write!(f, "{}", self.connection)?;
        Ok(())
    }
}

/// StageLocation (a.k.a internal and external stage) can be used
/// in `INTO` or `FROM`.
///
/// For examples:
///
/// - internal stage: `@internal_stage/path/to/dir/`
/// - external stage: `@s3_external_stage/path/to/dir/`
///
/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FileLocation {
    Stage(String),
    Uri(UriLocation),
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FileLocation::Uri(loc) => {
                write!(f, "{}", loc)
            }
            FileLocation::Stage(loc) => {
                write!(f, "'@{}'", loc)
            }
        }
    }
}

/// Used when we want to allow use variable for options etc.
/// Other expr is not necessary, because
/// 1. we can always create a variable that can be used directly.
/// 2. columns can not be referred.
///
/// Can extend to all type of Literals if needed later.
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum LiteralStringOrVariable {
    Literal(String),
    Variable(String),
}

impl Display for LiteralStringOrVariable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralStringOrVariable::Literal(s) => {
                write!(f, "'{s}'")
            }
            LiteralStringOrVariable::Variable(s) => {
                write!(f, "${s}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct FileFormatOptions {
    pub options: BTreeMap<String, FileFormatValue>,
}

impl FileFormatOptions {
    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }
}

impl Display for FileFormatOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write_comma_separated_map(f, &self.options)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Drive, DriveMut)]
pub enum FileFormatValue {
    Keyword(String),
    Bool(bool),
    U64(u64),
    String(String),
    StringList(Vec<String>),
}

impl FileFormatValue {
    pub fn to_meta_value(&self) -> String {
        match self {
            FileFormatValue::Keyword(v) => v.clone(),
            FileFormatValue::Bool(v) => v.to_string(),
            FileFormatValue::U64(v) => v.to_string(),
            FileFormatValue::String(v) => v.clone(),
            FileFormatValue::StringList(v) => serde_json::to_string(v).unwrap(),
        }
    }
}

impl Display for FileFormatValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FileFormatValue::Keyword(v) => write!(f, "{v}"),
            FileFormatValue::Bool(v) => write!(f, "{v}"),
            FileFormatValue::U64(v) => write!(f, "{v}"),
            FileFormatValue::String(v) => {
                write!(f, "{}", QuotedString(v, '\''))
            }
            FileFormatValue::StringList(v) => {
                write!(f, "(")?;
                for (i, s) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", QuotedString(s, '\''))?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Eq)]
pub enum OnErrorMode {
    Continue,
    SkipFileNum(u64),
    AbortNum(u64),
}

impl Default for OnErrorMode {
    fn default() -> Self {
        Self::AbortNum(1)
    }
}

impl Display for OnErrorMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OnErrorMode::Continue => {
                write!(f, "continue")
            }
            OnErrorMode::SkipFileNum(n) => {
                if *n <= 1 {
                    write!(f, "skipfile")
                } else {
                    write!(f, "skipfile_{}", n)
                }
            }
            OnErrorMode::AbortNum(n) => {
                if *n <= 1 {
                    write!(f, "abort")
                } else {
                    write!(f, "abort_{}", n)
                }
            }
        }
    }
}

const ERROR_MODE_MSG: &str =
    "OnError must one of {{ CONTINUE | SKIP_FILE | SKIP_FILE_<num> | ABORT | ABORT_<num> }}";
impl FromStr for OnErrorMode {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, &'static str> {
        match s.to_uppercase().as_str() {
            "" | "ABORT" => Ok(OnErrorMode::AbortNum(1)),
            "CONTINUE" => Ok(OnErrorMode::Continue),
            "SKIP_FILE" => Ok(OnErrorMode::SkipFileNum(1)),
            v => {
                if v.starts_with("ABORT_") {
                    let num_str = v.replace("ABORT_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => Err(ERROR_MODE_MSG),
                        Ok(n) => Ok(OnErrorMode::AbortNum(n)),
                        Err(_) => Err(ERROR_MODE_MSG),
                    }
                } else {
                    let num_str = v.replace("SKIP_FILE_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => Err(ERROR_MODE_MSG),
                        Ok(n) => Ok(OnErrorMode::SkipFileNum(n)),
                        Err(_) => Err(ERROR_MODE_MSG),
                    }
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Eq)]
pub enum ColumnMatchMode {
    CaseSensitive,
    CaseInsensitive,
    Position,
}

impl Display for ColumnMatchMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ColumnMatchMode::CaseSensitive => write!(f, "CASE_SENSITIVE"),
            ColumnMatchMode::CaseInsensitive => write!(f, "CASE_INSENSITIVE"),
            ColumnMatchMode::Position => write!(f, "POSITION"),
        }
    }
}

const COLUMN_MATCH_MODE_MSG: &str =
    "ColumnMatchMode must be one of {{ CASE_SENSITIVE | CASE_INSENSITIVE | POSITION }}";
impl FromStr for ColumnMatchMode {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, &'static str> {
        match s.to_uppercase().as_str() {
            "CASE_SENSITIVE" => Ok(Self::CaseSensitive),
            "CASE_INSENSITIVE" => Ok(Self::CaseInsensitive),
            "POSITION" => Ok(Self::Position),
            _ => Err(COLUMN_MATCH_MODE_MSG),
        }
    }
}
