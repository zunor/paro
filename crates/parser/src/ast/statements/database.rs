// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::statements::show::ShowLimit;
use crate::ast::write_dot_separated_list;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::SchemaRef;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowSchemasStmt {
    pub database: Option<Identifier>,
    pub full: bool,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowSchemasStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW ")?;
        if self.full {
            write!(f, "FULL ")?;
        }
        write!(f, "SCHEMAS")?;
        if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowDropSchemasStmt {
    pub database: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowDropSchemasStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW DROP SCHEMAS")?;
        if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowCreateSchemaStmt {
    pub database: Option<Identifier>,
    pub schema: Identifier,
}

impl Display for ShowCreateSchemaStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW CREATE SCHEMA ")?;
        write_dot_separated_list(f, self.database.iter().chain(Some(&self.schema)))?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateSchemaStmt {
    pub create_option: CreateOption,
    pub schema: SchemaRef,
    pub engine: Option<DatabaseEngine>,
    pub options: Vec<SQLProperty>,
}

impl Display for CreateSchemaStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "SCHEMA ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "{}", self.schema)?;

        if let Some(engine) = &self.engine {
            write!(f, " ENGINE = {engine}")?;
        }

        // TODO(leiysky): display rest information
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropSchemaStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Identifier,
    pub cascade: bool,
}

impl Display for DropSchemaStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SCHEMA ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(f, self.database.iter().chain(Some(&self.schema)))?;
        if self.cascade {
            write!(f, " CASCADE")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct UndropSchemaStmt {
    pub database: Option<Identifier>,
    pub schema: Identifier,
}

impl Display for UndropSchemaStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "UNDROP SCHEMA ")?;
        write_dot_separated_list(f, self.database.iter().chain(Some(&self.schema)))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterSchemaStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Identifier,
    pub action: AlterSchemaAction,
}

impl Display for AlterSchemaStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER SCHEMA ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(f, self.database.iter().chain(Some(&self.schema)))?;
        match &self.action {
            AlterSchemaAction::RenameSchema { new_schema } => {
                write!(f, " RENAME TO {new_schema}")?;
            }
            AlterSchemaAction::RefreshSchemaCache => {
                write!(f, " REFRESH CACHE")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AlterSchemaAction {
    RenameSchema { new_schema: Identifier },
    RefreshSchemaCache,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum DatabaseEngine {
    Default,
    Share,
}

impl Display for DatabaseEngine {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DatabaseEngine::Default => write!(f, "DEFAULT"),
            DatabaseEngine::Share => write!(f, "SHARE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct SQLProperty {
    pub name: String,
    pub value: String,
}
