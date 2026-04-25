// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_catalog::search_path::{CatalogSearchEntry, CatalogSetPathType};
use paro_common::chunk::Chunk;
use paro_common::config::format_setting_value;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::SettingRow;
use paro_execution::query_executor::compiled::ResultColumnDesc;
use paro_parser::ast::{
    SetType, SetValues, Settings, VariableSetKind, VariableSetStmt, VariableShowStmt,
    VariableShowTarget,
};

const DEFAULT_SEARCH_PATH_DISPLAY: &str = "\"$user\", public";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettingScope {
    Session,
    TransactionLocal,
}

type DefaultValueFn = fn(&Session) -> Value;
type ParseValueFn = fn(&Session, &[String]) -> Result<Value>;
type ApplySettingFn = fn(&mut Session, &Value) -> Result<()>;

#[derive(Clone, Copy)]
pub(crate) struct SettingDescriptor {
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub vartype: &'static str,
    pub context: &'static str,
    pub unit: Option<&'static str>,
    pub default_value: DefaultValueFn,
    pub parse_value: ParseValueFn,
    pub apply_effective: ApplySettingFn,
}

const SETTING_DESCRIPTORS: &[SettingDescriptor] = &[
    SettingDescriptor {
        name: "application_name",
        category: "Client Connection Defaults",
        description: "Name of the application connected to this session",
        vartype: "string",
        context: "user",
        unit: None,
        default_value: |_| Value::Varchar(String::new()),
        parse_value: parse_string_value,
        apply_effective: apply_application_name,
    },
    SettingDescriptor {
        name: "copy_buffer_size",
        category: "Client Connection Defaults",
        description: "Rows buffered before COPY flushes staged chunks",
        vartype: "integer",
        context: "user",
        unit: None,
        default_value: |_| Value::Integer(8192),
        parse_value: parse_positive_integer_value,
        apply_effective: apply_noop,
    },
    SettingDescriptor {
        name: "copy_flush_threads",
        category: "Client Connection Defaults",
        description: "Worker count used for COPY buffered flush",
        vartype: "integer",
        context: "user",
        unit: None,
        default_value: |_| Value::Integer(1),
        parse_value: parse_positive_integer_value,
        apply_effective: apply_noop,
    },
    SettingDescriptor {
        name: "default_table_cardinality",
        category: "Query Tuning",
        description: "Fallback table cardinality for the optimizer",
        vartype: "integer",
        context: "user",
        unit: None,
        default_value: |_| Value::BigInt(1000),
        parse_value: parse_positive_bigint_value,
        apply_effective: apply_noop,
    },
    SettingDescriptor {
        name: "force_external",
        category: "Query Tuning",
        description: "Force spill-capable operators onto external paths",
        vartype: "bool",
        context: "user",
        unit: None,
        default_value: |_| Value::Boolean(false),
        parse_value: parse_bool_value,
        apply_effective: apply_force_external,
    },
    SettingDescriptor {
        name: "max_temp_directory_size",
        category: "Resource Usage",
        description: "Maximum size of the temporary spill directory",
        vartype: "string",
        context: "user",
        unit: Some("bytes"),
        default_value: |session| match session
            .instance
            .boot_config()
            .initial_max_temp_directory_size
        {
            Some(limit) => Value::BigInt(limit as i64),
            None => Value::Varchar("unlimited".to_string()),
        },
        parse_value: parse_optional_bytes_value,
        apply_effective: apply_max_temp_directory_size,
    },
    SettingDescriptor {
        name: "memory_limit",
        category: "Resource Usage",
        description: "Maximum memory available to the current session",
        vartype: "string",
        context: "user",
        unit: Some("bytes"),
        default_value: |session| {
            Value::BigInt(session.instance.boot_config().initial_maximum_memory as i64)
        },
        parse_value: parse_bytes_value,
        apply_effective: apply_memory_limit,
    },
    SettingDescriptor {
        name: "optimizer_verify",
        category: "Developer Options",
        description: "Enable optimizer verification outside debug builds",
        vartype: "bool",
        context: "user",
        unit: None,
        default_value: |_| Value::Boolean(false),
        parse_value: parse_bool_value,
        apply_effective: apply_noop,
    },
    SettingDescriptor {
        name: "search_path",
        category: "Client Connection Defaults",
        description: "Sets the schema search order for names",
        vartype: "string",
        context: "user",
        unit: None,
        default_value: |_| Value::Varchar(DEFAULT_SEARCH_PATH_DISPLAY.to_string()),
        parse_value: parse_search_path_value,
        apply_effective: apply_search_path,
    },
    SettingDescriptor {
        name: "temp_directory",
        category: "Resource Usage",
        description: "Directory used for temporary spill files",
        vartype: "string",
        context: "user",
        unit: None,
        default_value: |session| {
            Value::Varchar(
                session
                    .instance
                    .boot_config()
                    .initial_temporary_directory
                    .clone(),
            )
        },
        parse_value: parse_string_value,
        apply_effective: apply_temp_directory,
    },
    SettingDescriptor {
        name: "statement_timeout",
        category: "Client Connection Defaults",
        description: "Maximum execution time for a single statement",
        vartype: "integer",
        context: "user",
        unit: Some("ms"),
        default_value: |_| Value::Integer(0),
        parse_value: parse_non_negative_integer_value,
        apply_effective: apply_noop,
    },
    SettingDescriptor {
        name: "threads",
        category: "Resource Usage",
        description: "Maximum number of execution threads for the session",
        vartype: "integer",
        context: "user",
        unit: None,
        default_value: |session| {
            Value::Integer(
                session
                    .instance
                    .runtime_tuning()
                    .snapshot()
                    .effective_max_threads() as i32,
            )
        },
        parse_value: parse_positive_integer_value,
        apply_effective: apply_threads,
    },
    SettingDescriptor {
        name: "use_new_agg_spill",
        category: "Developer Options",
        description: "Enable the newer hash aggregate spill path",
        vartype: "bool",
        context: "user",
        unit: None,
        default_value: |_| Value::Boolean(true),
        parse_value: parse_bool_value,
        apply_effective: apply_noop,
    },
];

pub(crate) fn initialize_setting_store(session: &mut Session) {
    session.config.clear_settings();
    session.effective_settings.clear();
    for descriptor in SETTING_DESCRIPTORS {
        session.effective_settings.insert(
            descriptor.name.to_string(),
            (descriptor.default_value)(session),
        );
    }
}

pub(crate) fn reconcile_effective_settings(session: &mut Session) -> Result<()> {
    let mut next_effective_settings = Vec::with_capacity(SETTING_DESCRIPTORS.len());

    for descriptor in SETTING_DESCRIPTORS {
        let value = effective_setting_value(session, descriptor);
        if session.effective_setting(descriptor.name) != Some(&value) {
            (descriptor.apply_effective)(session, &value)?;
        }
        next_effective_settings.push((descriptor.name.to_string(), value));
    }

    session.effective_settings = next_effective_settings.into_iter().collect();
    Ok(())
}

pub(crate) fn collect_setting_rows(session: &Session) -> Vec<SettingRow> {
    SETTING_DESCRIPTORS
        .iter()
        .map(|descriptor| {
            let value = effective_setting_value(session, descriptor);
            let source = if session.transaction.local_setting(descriptor.name).is_some() {
                "transaction_local"
            } else if session.config.get_setting(descriptor.name).is_some() {
                "session"
            } else {
                "default"
            };

            SettingRow {
                name: descriptor.name.to_string(),
                setting: render_setting_value(descriptor.name, &value),
                unit: descriptor.unit.map(str::to_string),
                category: descriptor.category.to_string(),
                short_desc: Some(descriptor.description.to_string()),
                source: source.to_string(),
                vartype: descriptor.vartype.to_string(),
                context: descriptor.context.to_string(),
            }
        })
        .collect()
}

pub(crate) async fn execute_variable_set<S: ResultSink>(
    session: &mut Session,
    stmt: &VariableSetStmt,
    sink: &mut S,
) -> Result<()> {
    match stmt.kind {
        VariableSetKind::Set => {
            let scope = resolve_setting_scope(stmt.settings.set_type)?;
            let (name, raw_values) = extract_setting_assignment(&stmt.settings)?;
            let descriptor = setting_descriptor(&name).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "unrecognized configuration parameter \"{name}\""
                ))
            })?;

            let is_default = raw_values.len() == 1 && raw_values[0].eq_ignore_ascii_case("default");
            match scope {
                SettingScope::Session => {
                    if is_default {
                        session.reset_session_setting(descriptor.name)?;
                    } else {
                        let value = (descriptor.parse_value)(session, &raw_values)?;
                        session.set_session_setting(descriptor.name, value)?;
                    }

                    sink.finish_result(&StatementCompletion::Set).await?;
                    return Ok(());
                }
                SettingScope::TransactionLocal => {
                    if !session.is_in_explicit_block() {
                        return Err(paro_error::invalid_transaction_state(
                            "SET LOCAL can only be used in transaction blocks".to_string(),
                        ));
                    }

                    if is_default {
                        session.transaction.set_local_setting(descriptor.name, None);
                    } else {
                        let value = (descriptor.parse_value)(session, &raw_values)?;
                        session
                            .transaction
                            .set_local_setting(descriptor.name, Some(value));
                    }
                }
            }

            reconcile_effective_settings(session)?;
            session.refresh_session_metadata();
            sink.finish_result(&StatementCompletion::Set).await?;
            Ok(())
        }
        VariableSetKind::Reset => {
            if stmt.settings.identifiers.len() != 1 {
                return Err(paro_error::not_supported(
                    "RESET currently only supports a single setting",
                ));
            }

            let name = stmt.settings.identifiers[0].name.to_lowercase();
            let descriptor = setting_descriptor(&name).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "unrecognized configuration parameter \"{name}\""
                ))
            })?;

            session.config.reset_setting(descriptor.name);
            reconcile_effective_settings(session)?;
            session.refresh_session_metadata();
            sink.finish_result(&StatementCompletion::Reset).await?;
            Ok(())
        }
        VariableSetKind::ResetAll => {
            session.config.clear_settings();
            reconcile_effective_settings(session)?;
            session.refresh_session_metadata();
            sink.finish_result(&StatementCompletion::Reset).await?;
            Ok(())
        }
    }
}

pub(crate) async fn execute_variable_show<S: ResultSink>(
    session: &mut Session,
    stmt: &VariableShowStmt,
    sink: &mut S,
) -> Result<()> {
    match &stmt.target {
        VariableShowTarget::All => {
            let rows = SETTING_DESCRIPTORS
                .iter()
                .map(|descriptor| {
                    vec![
                        descriptor.name.to_string(),
                        render_setting_value(
                            descriptor.name,
                            &effective_setting_value(session, descriptor),
                        ),
                        descriptor.description.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            emit_string_result(
                session,
                sink,
                &["name", "setting", "description"],
                &rows,
                StatementCompletion::Show,
            )
            .await
        }
        VariableShowTarget::Name(name) => {
            let descriptor = setting_descriptor(&name.name).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "unrecognized configuration parameter \"{}\"",
                    name.name
                ))
            })?;
            let rows = vec![vec![render_setting_value(
                descriptor.name,
                &effective_setting_value(session, descriptor),
            )]];
            emit_string_result(
                session,
                sink,
                &[descriptor.name],
                &rows,
                StatementCompletion::Show,
            )
            .await
        }
    }
}

pub(crate) fn describe_variable_show(stmt: &VariableShowStmt) -> Vec<ResultColumnDesc> {
    match &stmt.target {
        VariableShowTarget::All => vec![
            ResultColumnDesc::new("name", LogicalType::Varchar),
            ResultColumnDesc::new("setting", LogicalType::Varchar),
            ResultColumnDesc::new("description", LogicalType::Varchar),
        ],
        VariableShowTarget::Name(name) => {
            vec![ResultColumnDesc::new(
                name.name.to_lowercase(),
                LogicalType::Varchar,
            )]
        }
    }
}

pub(crate) fn render_setting_value(name: &str, value: &Value) -> String {
    format_setting_value(name, value)
}

fn setting_descriptor(name: &str) -> Option<&'static SettingDescriptor> {
    let name = name.to_lowercase();
    SETTING_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.name == name)
}

fn effective_setting_value(session: &Session, descriptor: &SettingDescriptor) -> Value {
    session
        .transaction
        .local_setting(descriptor.name)
        .cloned()
        .or_else(|| session.config.get_setting(descriptor.name).cloned())
        .unwrap_or_else(|| (descriptor.default_value)(session))
}

fn resolve_setting_scope(set_type: SetType) -> Result<SettingScope> {
    match set_type {
        SetType::SettingsQuery | SetType::SettingsSession => Ok(SettingScope::Session),
        SetType::SettingsLocal => Ok(SettingScope::TransactionLocal),
        SetType::SettingsGlobal => Err(paro_error::not_supported("SET GLOBAL is not supported")),
        SetType::Variable => Err(paro_error::not_supported("SET VARIABLE is not supported")),
    }
}

fn extract_setting_assignment(settings: &Settings) -> Result<(String, Vec<String>)> {
    if settings.identifiers.len() != 1 {
        return Err(paro_error::not_supported(
            "SET currently only supports a single setting target",
        ));
    }

    let name = settings.identifiers[0].name.to_lowercase();
    let values = match &settings.values {
        SetValues::Expr(values) if !values.is_empty() => values
            .iter()
            .map(|value| normalize_setting_text(&value.to_string()))
            .collect(),
        SetValues::Expr(_) => {
            return Err(paro_error::not_supported("SET requires at least one value"))
        }
        SetValues::Query(_) => {
            return Err(paro_error::not_supported(
                "SET ... = SELECT is not supported for PostgreSQL-compatible settings",
            ))
        }
        SetValues::None => return Err(paro_error::not_supported("SET requires an explicit value")),
    };

    Ok((name, values))
}

fn normalize_setting_text(text: &str) -> String {
    text.trim()
        .trim_matches('\'')
        .trim_matches('"')
        .trim()
        .to_string()
}

fn parse_string_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single scalar value".to_string(),
        ));
    }
    Ok(Value::Varchar(values[0].clone()))
}

fn parse_positive_integer_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single positive integer".to_string(),
        ));
    }

    let parsed: i64 = values[0].parse().map_err(|_| {
        paro_error::invalid_input(format!("invalid positive integer value '{}'", values[0]))
    })?;
    if parsed < 1 {
        return Err(paro_error::invalid_input(format!(
            "invalid positive integer value '{}'",
            values[0]
        )));
    }
    Ok(Value::Integer(parsed as i32))
}

fn parse_non_negative_integer_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single non-negative integer".to_string(),
        ));
    }

    let parsed: i64 = values[0].parse().map_err(|_| {
        paro_error::invalid_input(format!(
            "invalid non-negative integer value '{}'",
            values[0]
        ))
    })?;
    if parsed < 0 {
        return Err(paro_error::invalid_input(format!(
            "invalid non-negative integer value '{}'",
            values[0]
        )));
    }
    Ok(Value::Integer(parsed as i32))
}

fn parse_positive_bigint_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single positive integer".to_string(),
        ));
    }

    let parsed: i64 = values[0].parse().map_err(|_| {
        paro_error::invalid_input(format!("invalid positive integer value '{}'", values[0]))
    })?;
    if parsed < 1 {
        return Err(paro_error::invalid_input(format!(
            "invalid positive integer value '{}'",
            values[0]
        )));
    }
    Ok(Value::BigInt(parsed))
}

fn parse_bool_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single boolean value".to_string(),
        ));
    }

    let normalized = values[0].to_ascii_lowercase();
    match normalized.as_str() {
        "true" | "on" | "1" | "yes" => Ok(Value::Boolean(true)),
        "false" | "off" | "0" | "no" => Ok(Value::Boolean(false)),
        _ => Err(paro_error::invalid_input(format!(
            "invalid boolean value '{}'",
            values[0]
        ))),
    }
}

fn parse_bytes_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single size value".to_string(),
        ));
    }

    let parsed = paro_common::config::parse_human_bytes(&values[0]).map_err(|e| {
        paro_error::invalid_input(format!("invalid size value '{}': {e}", values[0]))
    })?;
    Ok(Value::BigInt(parsed as i64))
}

fn parse_optional_bytes_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.len() != 1 {
        return Err(paro_error::invalid_input(
            "expected a single size value".to_string(),
        ));
    }

    match values[0].to_ascii_lowercase().as_str() {
        "none" | "null" | "unlimited" => Ok(Value::Varchar("unlimited".to_string())),
        _ => {
            let parsed = paro_common::config::parse_human_bytes(&values[0]).map_err(|e| {
                paro_error::invalid_input(format!("invalid size value '{}': {e}", values[0]))
            })?;
            Ok(Value::BigInt(parsed as i64))
        }
    }
}

fn parse_search_path_value(_session: &Session, values: &[String]) -> Result<Value> {
    if values.is_empty() {
        return Err(paro_error::invalid_input(
            "search_path requires at least one entry".to_string(),
        ));
    }

    let rendered = values
        .iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Value::Varchar(rendered))
}

fn apply_noop(_session: &mut Session, _value: &Value) -> Result<()> {
    Ok(())
}

fn apply_application_name(session: &mut Session, value: &Value) -> Result<()> {
    let Value::Varchar(value) = value else {
        return Err(paro_error::internal(
            "application_name must be a string".to_string(),
        ));
    };
    session.state.application_name = value.clone();
    Ok(())
}

fn apply_search_path(session: &mut Session, value: &Value) -> Result<()> {
    let Value::Varchar(value) = value else {
        return Err(paro_error::internal(
            "search_path must be a string".to_string(),
        ));
    };

    if value.trim().is_empty() || value.eq_ignore_ascii_case(DEFAULT_SEARCH_PATH_DISPLAY) {
        session.state.search_path_mut().reset();
        return Ok(());
    }

    let entries = CatalogSearchEntry::parse_list(value)?
        .into_iter()
        .filter(|entry| !entry.schema.eq_ignore_ascii_case("$user"))
        .collect::<Vec<_>>();

    if entries.is_empty() {
        session.state.search_path_mut().reset();
    } else {
        session
            .state
            .search_path_mut()
            .set(entries, CatalogSetPathType::SetSearchPath)?;
    }
    Ok(())
}

fn apply_threads(session: &mut Session, value: &Value) -> Result<()> {
    let threads = value_to_usize(value, "threads")?;
    let default_threads = session
        .instance
        .runtime_tuning()
        .snapshot()
        .effective_max_threads();
    if threads == default_threads {
        session.config.reset_threads();
    } else {
        session.config.set_threads(threads);
    }
    session.instance.set_threads(threads)?;
    Ok(())
}

fn apply_memory_limit(session: &mut Session, value: &Value) -> Result<()> {
    let limit = value_to_usize(value, "memory_limit")?;
    session.instance.set_memory_limit(limit)?;
    Ok(())
}

fn apply_temp_directory(session: &mut Session, value: &Value) -> Result<()> {
    let Value::Varchar(value) = value else {
        return Err(paro_error::internal(
            "temp_directory must be a string".to_string(),
        ));
    };
    session.instance.set_temporary_directory(value.clone())?;
    Ok(())
}

fn apply_max_temp_directory_size(session: &mut Session, value: &Value) -> Result<()> {
    match value {
        Value::Varchar(value) if value.eq_ignore_ascii_case("unlimited") => {
            session.instance.set_max_temp_directory_size(None)?;
            Ok(())
        }
        _ => {
            let limit = value_to_usize(value, "max_temp_directory_size")?;
            session.instance.set_max_temp_directory_size(Some(limit))?;
            Ok(())
        }
    }
}

fn apply_force_external(session: &mut Session, value: &Value) -> Result<()> {
    let enabled = value_to_bool(value, "force_external")?;
    session.config.force_external = enabled;
    Ok(())
}

fn value_to_usize(value: &Value, setting: &str) -> Result<usize> {
    match value {
        Value::Integer(v) if *v > 0 => Ok(*v as usize),
        Value::BigInt(v) if *v > 0 => Ok(*v as usize),
        Value::Varchar(v) => {
            let parsed: i64 = v.parse().map_err(|_| {
                paro_error::invalid_input(format!(
                    "invalid value for {setting}: '{v}'. Expected a positive integer.",
                ))
            })?;
            if parsed < 1 {
                return Err(paro_error::invalid_input(format!(
                    "invalid value for {setting}: '{v}'. Expected a positive integer.",
                )));
            }
            Ok(parsed as usize)
        }
        _ => Err(paro_error::invalid_input(format!(
            "invalid value for {setting}: '{value}'. Expected a positive integer.",
        ))),
    }
}

fn value_to_bool(value: &Value, setting: &str) -> Result<bool> {
    match value {
        Value::Boolean(value) => Ok(*value),
        Value::Varchar(value) => {
            let normalized = value.to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "on" | "1" | "yes" => Ok(true),
                "false" | "off" | "0" | "no" => Ok(false),
                _ => Err(paro_error::invalid_input(format!(
                    "invalid value for {setting}: '{value}'. Expected a boolean.",
                ))),
            }
        }
        _ => Err(paro_error::invalid_input(format!(
            "invalid value for {setting}: '{value}'. Expected a boolean.",
        ))),
    }
}

async fn emit_string_result<S: ResultSink>(
    session: &Session,
    sink: &mut S,
    names: &[&str],
    rows: &[Vec<String>],
    completion: StatementCompletion,
) -> Result<()> {
    let names = names
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let types = vec![LogicalType::Varchar; names.len()];
    sink.start_result(&names, &types).await?;

    if !rows.is_empty() {
        let allocator = session.buffer_allocator();
        let mut vectors = Vec::with_capacity(names.len());
        for col_idx in 0..names.len() {
            let values = rows
                .iter()
                .map(|row| row[col_idx].as_str())
                .collect::<Vec<_>>();
            vectors.push(Vector::try_from_strings(&values, allocator.clone())?);
        }
        let chunk = Chunk::from_vectors(vectors, allocator);
        sink.push_chunk(&chunk).await?;
    }

    sink.finish_result(&completion).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        collect_setting_rows, execute_variable_set, execute_variable_show,
        initialize_setting_store, render_setting_value,
    };
    use crate::result::collecting_sink::CollectingSink;
    use paro_common::runtime_value::Value;

    #[tokio::test]
    async fn show_all_writes_rows_directly() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = crate::Session::new(1, instance);
        let mut sink = CollectingSink::new();

        execute_variable_show(
            &mut session,
            &paro_parser::ast::VariableShowStmt {
                target: paro_parser::ast::VariableShowTarget::All,
            },
            &mut sink,
        )
        .await
        .unwrap();

        let result = sink.assert_single_result();
        assert_eq!(result.names, vec!["name", "setting", "description"]);
        assert!(!result.chunks.is_empty());
    }

    #[tokio::test]
    async fn set_local_requires_explicit_transaction() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = crate::Session::new(1, instance);
        let mut sink = CollectingSink::new();
        let stmt = match paro_parser::parse("SET LOCAL application_name = 'x'")
            .unwrap()
            .remove(0)
            .stmt
        {
            paro_parser::ast::Statement::VariableSet(stmt) => stmt,
            other => panic!("expected variable set statement, got {other:?}"),
        };

        let err = execute_variable_set(&mut session, &stmt, &mut sink)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("SET LOCAL"));
    }

    #[test]
    fn initialize_store_populates_builtin_defaults() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = crate::Session::new(1, instance);
        session.config.clear_settings();

        initialize_setting_store(&mut session);

        assert!(session.effective_setting("application_name").is_some());
        assert!(session.effective_setting("search_path").is_some());
    }

    #[test]
    fn metadata_rows_use_effective_values() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = crate::Session::new(1, instance);
        session
            .config
            .set_setting("application_name", Value::Varchar("base".to_string()));
        session.transaction.set_local_setting(
            "application_name",
            Some(Value::Varchar("local".to_string())),
        );

        let rows = collect_setting_rows(&session);
        let app_name = rows
            .into_iter()
            .find(|row| row.name == "application_name")
            .unwrap();
        assert_eq!(app_name.setting, "local");
        assert_eq!(app_name.source, "transaction_local");
    }

    #[test]
    fn byte_settings_render_with_human_units() {
        assert_eq!(
            render_setting_value("memory_limit", &Value::BigInt(1_000_000_000)),
            "1GB"
        );
        assert_eq!(
            render_setting_value("max_temp_directory_size", &Value::BigInt(512_000_000)),
            "512MB"
        );
    }
}
