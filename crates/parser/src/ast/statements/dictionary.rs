// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use super::ShowLimit;
use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::ColumnDefinition;
use crate::ast::CreateOption;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateDictionaryStmt {
    pub create_option: CreateOption,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub dictionary_name: Identifier,
    pub columns: Vec<ColumnDefinition>,
    pub primary_keys: Vec<Identifier>,
    pub source_name: Identifier,
    pub source_options: BTreeMap<String, String>,
    pub comment: Option<String>,
}

impl Display for CreateDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "DICTIONARY ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.dictionary_name)),
        )?;
        if !self.columns.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        if !self.primary_keys.is_empty() {
            write!(f, "PRIMARY KEY ")?;
            write_comma_separated_list(f, &self.primary_keys)?;
        }
        write!(f, " SOURCE(")?;
        write!(f, "{}( ", &self.source_name)?;
        if !self.source_options.is_empty() {
            write_space_separated_string_map(f, &self.source_options)?;
        }
        write!(f, ")")?;
        write!(f, ")")?;
        if let Some(comment) = &self.comment {
            write!(f, "COMMENT {} ", QuotedString(comment.clone(), '\''))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropDictionaryStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub dictionary_name: Identifier,
}

impl Display for DropDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP DICTIONARY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.dictionary_name)),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowCreateDictionaryStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub dictionary_name: Identifier,
}

impl Display for ShowCreateDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW CREATE DICTIONARY ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.dictionary_name)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowDictionariesStmt {
    pub schema: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowDictionariesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW DICTIONARIES")?;
        if let Some(schema) = &self.schema {
            write!(f, " FROM {schema}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct RenameDictionaryStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub dictionary: Identifier,
    pub new_database: Option<Identifier>,
    pub new_schema: Option<Identifier>,
    pub new_dictionary: Identifier,
}

impl Display for RenameDictionaryStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RENAME DICTIONARY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.dictionary)),
        )?;
        write!(f, " TO ")?;
        write_dot_separated_list(
            f,
            self.new_database
                .iter()
                .chain(&self.new_schema)
                .chain(Some(&self.new_dictionary)),
        )
    }
}
