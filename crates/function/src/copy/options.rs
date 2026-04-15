// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::CopyOptionValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    pub delimiter: Option<String>,
    pub null_string: Option<String>,
    pub header: Option<bool>,
    pub quote: Option<char>,
    pub escape: Option<char>,
    pub force_quote: ForceQuoteOption,
    pub force_not_null: Vec<String>,
    pub force_null: Vec<String>,
    pub encoding: Option<String>,
    pub freeze: bool,
    pub per_thread_output: bool,
    pub parallel: bool,
    pub parallel_workers: Option<usize>,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            delimiter: None,
            null_string: None,
            header: None,
            quote: None,
            escape: None,
            force_quote: ForceQuoteOption::None,
            force_not_null: Vec::new(),
            force_null: Vec::new(),
            encoding: None,
            freeze: false,
            per_thread_output: false,
            parallel: false,
            parallel_workers: None,
        }
    }
}

impl CopyOptions {
    pub fn from_ast(options: &[(String, CopyOptionValue)]) -> Result<Self> {
        let mut result = CopyOptions::default();
        for (key, value) in options {
            result.apply_option(key, value)?;
        }
        result.apply_format_defaults();
        Ok(result)
    }

    fn apply_option(&mut self, key: &str, value: &CopyOptionValue) -> Result<()> {
        let key = key.to_lowercase();
        if matches!(value, CopyOptionValue::Default) {
            return self.apply_default(&key);
        }

        match key.as_str() {
            "format" => {
                let format_str = Self::parse_string(value, "format")?;
                self.format = CopyFormat::parse(&format_str)?;
            }
            "delimiter" => {
                self.delimiter = Some(Self::parse_string(value, "delimiter")?);
            }
            "null" | "null_string" => {
                self.null_string = Some(Self::parse_string(value, "null")?);
            }
            "header" => {
                self.header = Some(Self::parse_bool(value, "header")?);
            }
            "quote" => {
                let s = Self::parse_string(value, "quote")?;
                self.quote = Some(Self::parse_char("quote", &s)?);
            }
            "escape" => {
                let s = Self::parse_string(value, "escape")?;
                self.escape = Some(Self::parse_char("escape", &s)?);
            }
            "force_quote" => {
                self.force_quote = Self::parse_force_quote(value)?;
            }
            "force_not_null" => {
                self.force_not_null = Self::parse_list(value, "force_not_null")?;
            }
            "force_null" => {
                self.force_null = Self::parse_list(value, "force_null")?;
            }
            "encoding" => {
                self.encoding = Some(Self::parse_string(value, "encoding")?);
            }
            "freeze" => {
                self.freeze = Self::parse_bool(value, "freeze")?;
            }
            "per_thread_output" => {
                self.per_thread_output = Self::parse_bool(value, "per_thread_output")?;
            }
            "parallel" => {
                self.parallel = Self::parse_bool(value, "parallel")?;
            }
            "parallel_workers" => {
                self.parallel_workers = Some(Self::parse_usize(value, "parallel_workers")?);
            }
            _ => {
                return Err(paro_error::invalid_parameter(format!(
                    "unknown COPY option: {}",
                    key
                )));
            }
        }

        Ok(())
    }

    fn apply_default(&mut self, key: &str) -> Result<()> {
        match key {
            "format" => self.format = CopyFormat::Text,
            "delimiter" => self.delimiter = None,
            "null" | "null_string" => self.null_string = None,
            "header" => self.header = None,
            "quote" => self.quote = None,
            "escape" => self.escape = None,
            "force_quote" => self.force_quote = ForceQuoteOption::None,
            "force_not_null" => self.force_not_null.clear(),
            "force_null" => self.force_null.clear(),
            "encoding" => self.encoding = None,
            "freeze" => self.freeze = false,
            "per_thread_output" => self.per_thread_output = false,
            "parallel" => self.parallel = false,
            "parallel_workers" => self.parallel_workers = None,
            _ => {
                return Err(paro_error::invalid_parameter(format!(
                    "unknown COPY option: {}",
                    key
                )));
            }
        }
        Ok(())
    }

    fn apply_format_defaults(&mut self) {
        if self.header.is_none() {
            self.header = Some(false);
        }

        match self.format {
            CopyFormat::Text => {
                if self.delimiter.is_none() {
                    self.delimiter = Some("\t".to_string());
                }
                if self.null_string.is_none() {
                    self.null_string = Some("\\N".to_string());
                }
            }
            CopyFormat::Csv => {
                if self.delimiter.is_none() {
                    self.delimiter = Some(",".to_string());
                }
                if self.null_string.is_none() {
                    self.null_string = Some(String::new());
                }
                if self.quote.is_none() {
                    self.quote = Some('"');
                }
                if self.escape.is_none() {
                    self.escape = Some('"');
                }
            }
            CopyFormat::Binary | CopyFormat::Ndjson => {}
        }
    }

    fn parse_string(value: &CopyOptionValue, key: &str) -> Result<String> {
        match value {
            CopyOptionValue::String(s) => Ok(s.clone()),
            CopyOptionValue::Number(n) => Ok(n.to_string()),
            _ => Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a string value",
                key
            ))),
        }
    }

    fn parse_bool(value: &CopyOptionValue, key: &str) -> Result<bool> {
        match value {
            CopyOptionValue::Boolean(v) => Ok(*v),
            CopyOptionValue::String(v) => match v.to_lowercase().as_str() {
                "true" | "t" | "1" => Ok(true),
                "false" | "f" | "0" => Ok(false),
                _ => Err(paro_error::invalid_parameter(format!(
                    "COPY option {} expects a boolean value",
                    key
                ))),
            },
            _ => Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a boolean value",
                key
            ))),
        }
    }

    fn parse_list(value: &CopyOptionValue, key: &str) -> Result<Vec<String>> {
        match value {
            CopyOptionValue::List(values) => Ok(values.clone()),
            _ => Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a list value",
                key
            ))),
        }
    }

    fn parse_char(key: &str, value: &str) -> Result<char> {
        let mut chars = value.chars();
        let Some(ch) = chars.next() else {
            return Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a single character",
                key
            )));
        };
        if chars.next().is_some() {
            return Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a single character",
                key
            )));
        }
        Ok(ch)
    }

    fn parse_force_quote(value: &CopyOptionValue) -> Result<ForceQuoteOption> {
        match value {
            CopyOptionValue::Star => Ok(ForceQuoteOption::All),
            CopyOptionValue::List(values) => Ok(ForceQuoteOption::Columns(values.clone())),
            _ => Err(paro_error::invalid_parameter(
                "COPY option force_quote expects '*' or a column list",
            )),
        }
    }

    fn parse_usize(value: &CopyOptionValue, key: &str) -> Result<usize> {
        let parsed = match value {
            CopyOptionValue::Number(v) => *v,
            CopyOptionValue::String(v) => v.parse::<u64>().map_err(|_| {
                paro_error::invalid_parameter(format!(
                    "COPY option {} expects a positive integer value",
                    key
                ))
            })?,
            _ => {
                return Err(paro_error::invalid_parameter(format!(
                    "COPY option {} expects a positive integer value",
                    key
                )))
            }
        };

        if parsed == 0 {
            return Err(paro_error::invalid_parameter(format!(
                "COPY option {} expects a value >= 1",
                key
            )));
        }

        usize::try_from(parsed).map_err(|_| {
            paro_error::invalid_parameter(format!(
                "COPY option {} is out of range for this platform",
                key
            ))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
    Ndjson,
}

impl CopyFormat {
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "text" => Ok(CopyFormat::Text),
            "csv" => Ok(CopyFormat::Csv),
            "binary" => Ok(CopyFormat::Binary),
            "ndjson" | "json" => Ok(CopyFormat::Ndjson),
            _ => Err(paro_error::invalid_parameter(format!(
                "unknown COPY format: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForceQuoteOption {
    None,
    All,
    Columns(Vec<String>),
}
