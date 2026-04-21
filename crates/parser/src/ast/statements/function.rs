// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct FunctionName {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub name: Identifier,
}

impl Display for FunctionName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{database}.")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct FunctionArgument {
    pub name: Option<Identifier>,
    pub data_type: TypeName,
}

impl Display for FunctionArgument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{name} {}", self.data_type)
        } else {
            write!(f, "{}", self.data_type)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct FunctionTableColumn {
    pub name: Identifier,
    pub data_type: TypeName,
}

impl Display for FunctionTableColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum FunctionReturn {
    Scalar(TypeName),
    Table(Vec<FunctionTableColumn>),
}

impl Display for FunctionReturn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionReturn::Scalar(ty) => write!(f, "{ty}"),
            FunctionReturn::Table(columns) => {
                write!(f, "TABLE (")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}

impl Display for FunctionVolatility {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionVolatility::Immutable => write!(f, "IMMUTABLE"),
            FunctionVolatility::Stable => write!(f, "STABLE"),
            FunctionVolatility::Volatile => write!(f, "VOLATILE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FunctionSecurity {
    Invoker,
    Definer,
}

impl Display for FunctionSecurity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionSecurity::Invoker => write!(f, "SECURITY INVOKER"),
            FunctionSecurity::Definer => write!(f, "SECURITY DEFINER"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateFunctionStmt {
    pub create_option: CreateOption,
    pub name: FunctionName,
    pub arguments: Vec<FunctionArgument>,
    pub return_type: FunctionReturn,
    pub language: Identifier,
    pub volatility: Option<FunctionVolatility>,
    pub strict: bool,
    pub security: FunctionSecurity,
    pub handler: Option<String>,
    pub packages: Vec<String>,
    pub imports: Vec<String>,
    pub rows: Option<u64>,
    pub capability_profile: Option<Identifier>,
    pub definition: String,
}

impl Display for CreateFunctionStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " FUNCTION")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}(", self.name)?;
        write_comma_separated_list(f, &self.arguments)?;
        write!(f, ") RETURNS {}", self.return_type)?;
        write!(f, " LANGUAGE {}", self.language)?;
        if let Some(volatility) = &self.volatility {
            write!(f, " {volatility}")?;
        }
        if self.strict {
            write!(f, " STRICT")?;
        }
        if self.security != FunctionSecurity::Invoker {
            write!(f, " {}", self.security)?;
        }
        if let Some(handler) = &self.handler {
            write!(f, " HANDLER {}", QuotedString(handler, '\''))?;
        }
        if !self.packages.is_empty() {
            let packages = self
                .packages
                .iter()
                .map(|value| QuotedString(value, '\'').to_string())
                .join(", ");
            write!(f, " PACKAGES ({packages})")?;
        }
        if !self.imports.is_empty() {
            let imports = self
                .imports
                .iter()
                .map(|value| QuotedString(value, '\'').to_string())
                .join(", ");
            write!(f, " IMPORTS ({imports})")?;
        }
        if let Some(rows) = self.rows {
            write!(f, " ROWS {rows}")?;
        }
        if let Some(profile) = &self.capability_profile {
            write!(f, " CAPABILITY PROFILE {profile}")?;
        }
        write!(f, " AS $$\n{}\n$$", self.definition)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct FunctionIdentity {
    pub name: FunctionName,
    pub arg_types: Vec<TypeName>,
}

impl Display for FunctionIdentity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;
        write_comma_separated_list(f, &self.arg_types)?;
        write!(f, ")")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropFunctionStmt {
    pub if_exists: bool,
    pub identity: FunctionIdentity,
}

impl Display for DropFunctionStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP FUNCTION ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.identity)
    }
}
