// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Result sinks and shared row/error encoding helpers for PostgreSQL wire messages.

use async_trait::async_trait;
use futures::SinkExt;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_execution::query_executor::compiled::ResultColumnDesc;
use paro_session::{
    encode_binary_value, FormatCode, ProtocolResultSink, ResultSink, StatementCompletion,
};
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpStream;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::Framed;

use crate::client_connection::PgCodec;

const FORMAT_CODE_TEXT: i16 = 0;
const NO_TABLE_ID: i32 = 0;
const NO_COLUMN_ID: i16 = 0;

pub struct PgWireResultSink<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    col_count: usize,
}

impl<'a> PgWireResultSink<'a> {
    pub fn new(socket: &'a mut Framed<TcpStream, PgCodec>) -> Self {
        Self {
            socket,
            col_count: 0,
        }
    }

    pub(crate) fn socket_mut(&mut self) -> &mut Framed<TcpStream, PgCodec> {
        self.socket
    }
}

#[async_trait]
impl<'a> ResultSink for PgWireResultSink<'a> {
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.col_count = names.len();

        let fields = names
            .iter()
            .zip(types)
            .map(|(name, logical_type)| field_description(name.clone(), logical_type))
            .collect::<Vec<_>>();

        self.socket
            .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                fields,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;

        Ok(())
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        send_text_chunk_rows(self.socket, chunk, self.col_count).await
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                completion.to_command_complete(),
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;

        self.col_count = 0;
        Ok(())
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::ErrorResponse(build_error_response(
                err,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }
}

impl<'a> ProtocolResultSink for PgWireResultSink<'a> {}

pub(crate) fn field_description(name: String, logical_type: &LogicalType) -> FieldDescription {
    field_description_with_format(name, logical_type, FORMAT_CODE_TEXT)
}

pub(crate) fn field_description_with_format(
    name: String,
    logical_type: &LogicalType,
    format_code: i16,
) -> FieldDescription {
    let descriptor = logical_type.pg_descriptor();
    FieldDescription::new(
        name,
        NO_TABLE_ID,
        NO_COLUMN_ID,
        descriptor.oid,
        descriptor.type_size,
        descriptor.type_modifier,
        format_code,
    )
}

pub(crate) async fn send_text_chunk_rows(
    socket: &mut Framed<TcpStream, PgCodec>,
    chunk: &Chunk,
    col_count: usize,
) -> Result<()> {
    for row_idx in 0..chunk.size() {
        let mut buf = BytesMut::new();
        for col_idx in 0..col_count {
            if let Some(vector) = chunk.column(col_idx) {
                if vector.is_null(row_idx) {
                    buf.extend_from_slice(&(-1_i32).to_be_bytes());
                    continue;
                }
                let value = vector.get_value(row_idx);
                let text = value_to_pg_text(&value);
                let bytes = text.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
            } else {
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
            }
        }
        socket
            .send(PgWireBackendMessage::DataRow(DataRow::new(
                buf,
                col_count as i16,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
    }
    Ok(())
}

pub(crate) async fn send_chunk_rows(
    socket: &mut Framed<TcpStream, PgCodec>,
    chunk: &Chunk,
    schema: &[ResultColumnDesc],
    format_codes: &[FormatCode],
) -> Result<()> {
    for row_idx in 0..chunk.size() {
        let mut buf = BytesMut::new();
        for (col_idx, column) in schema.iter().enumerate() {
            if let Some(vector) = chunk.column(col_idx) {
                if vector.is_null(row_idx) {
                    buf.extend_from_slice(&(-1_i32).to_be_bytes());
                    continue;
                }
                let value = vector.get_value(row_idx);
                let payload = match format_codes.get(col_idx).unwrap_or(&FormatCode::Text) {
                    FormatCode::Text => value_to_pg_text(&value).into_bytes(),
                    FormatCode::Binary => encode_binary_value(&value, &column.logical_type)?,
                };
                buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
                buf.extend_from_slice(&payload);
            } else {
                buf.extend_from_slice(&(-1_i32).to_be_bytes());
            }
        }
        socket
            .send(PgWireBackendMessage::DataRow(DataRow::new(
                buf,
                schema.len() as i16,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
    }
    Ok(())
}

pub(crate) fn build_error_response(err: &ParoError) -> pgwire::messages::response::ErrorResponse {
    let data = err.data();
    build_error_response_fields(ErrorResponseFields {
        severity: data.severity.as_str(),
        sqlstate: data.sqlstate.as_str(),
        message: data.message.as_ref(),
        detail: data.detail.as_deref(),
        hint: data.hint.as_deref(),
        position: data.position,
        schema_name: data.schema_name.as_deref(),
        table_name: data.table_name.as_deref(),
        column_name: data.column_name.as_deref(),
        datatype_name: data.datatype_name.as_deref(),
        constraint_name: data.constraint_name.as_deref(),
    })
}

pub(crate) fn build_error_response_message(
    severity: &str,
    sqlstate: &str,
    message: &str,
) -> pgwire::messages::response::ErrorResponse {
    build_error_response_fields(ErrorResponseFields {
        severity,
        sqlstate,
        message,
        detail: None,
        hint: None,
        position: None,
        schema_name: None,
        table_name: None,
        column_name: None,
        datatype_name: None,
        constraint_name: None,
    })
}

struct ErrorResponseFields<'a> {
    severity: &'a str,
    sqlstate: &'a str,
    message: &'a str,
    detail: Option<&'a str>,
    hint: Option<&'a str>,
    position: Option<u32>,
    schema_name: Option<&'a str>,
    table_name: Option<&'a str>,
    column_name: Option<&'a str>,
    datatype_name: Option<&'a str>,
    constraint_name: Option<&'a str>,
}

fn build_error_response_fields(
    fields_in: ErrorResponseFields<'_>,
) -> pgwire::messages::response::ErrorResponse {
    let mut fields: Vec<(u8, String)> = Vec::with_capacity(16);
    fields.push((b'S', fields_in.severity.to_string()));
    fields.push((b'C', fields_in.sqlstate.to_string()));
    fields.push((b'M', fields_in.message.to_string()));

    if let Some(detail) = fields_in.detail {
        fields.push((b'D', detail.to_string()));
    }
    if let Some(hint) = fields_in.hint {
        fields.push((b'H', hint.to_string()));
    }
    if let Some(position) = fields_in.position {
        fields.push((b'P', position.to_string()));
    }
    if let Some(schema) = fields_in.schema_name {
        fields.push((b's', schema.to_string()));
    }
    if let Some(table) = fields_in.table_name {
        fields.push((b't', table.to_string()));
    }
    if let Some(column) = fields_in.column_name {
        fields.push((b'c', column.to_string()));
    }
    if let Some(datatype) = fields_in.datatype_name {
        fields.push((b'd', datatype.to_string()));
    }
    if let Some(constraint) = fields_in.constraint_name {
        fields.push((b'n', constraint.to_string()));
    }

    pgwire::messages::response::ErrorResponse::new(fields)
}

pub(crate) fn value_to_pg_text(value: &Value) -> String {
    match value {
        Value::HugeInt(v) => v.to_string(),
        Value::UBigInt(v) => v.to_string(),
        Value::UHugeInt(v) => v.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::List(values, _) | Value::Array(values, _, _) => format_pg_array(values),
        _ => value.to_string(),
    }
}

pub(crate) fn format_pg_array(values: &[Value]) -> String {
    let mut out = String::from("{");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format_pg_array_element(value));
    }
    out.push('}');
    out
}

fn format_pg_array_element(value: &Value) -> String {
    match value {
        Value::Null(_) => "NULL".to_string(),
        Value::Varchar(s) => format_pg_array_string(s),
        Value::List(values, _) | Value::Array(values, _, _) => format_pg_array(values),
        _ => value_to_pg_text(value),
    }
}

fn format_pg_array_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::pg_oid::{INT2OID, INT4OID, NUMERICOID};
    use paro_session::FormatCode;

    #[test]
    fn field_descriptions_use_pg_descriptor_metadata() {
        let int_field = field_description("i".to_string(), &LogicalType::TinyInt);
        assert_eq!(int_field.type_id, INT2OID);
        assert_eq!(int_field.type_size, 2);
        assert_eq!(int_field.type_modifier, -1);

        let numeric_field = field_description("n".to_string(), &LogicalType::HugeInt);
        assert_eq!(numeric_field.type_id, NUMERICOID);
        assert_eq!(numeric_field.type_size, -1);

        let literal_field = field_description("lit".to_string(), &LogicalType::IntegerLiteral(1));
        assert_eq!(literal_field.type_id, INT4OID);
        assert_eq!(literal_field.type_size, 4);
    }

    #[test]
    fn value_to_pg_text_formats_large_integers_as_decimal() {
        assert_eq!(
            value_to_pg_text(&Value::HugeInt(i128::MAX)),
            i128::MAX.to_string()
        );
        assert_eq!(
            value_to_pg_text(&Value::UBigInt(u64::MAX)),
            u64::MAX.to_string()
        );
        assert_eq!(
            value_to_pg_text(&Value::UHugeInt(u128::MAX)),
            u128::MAX.to_string()
        );
    }

    #[test]
    fn row_descriptions_preserve_requested_format_codes() {
        let field = field_description_with_format("v".to_string(), &LogicalType::Integer, 1);
        assert_eq!(field.format_code, 1);
        assert_eq!(
            field_description_with_format("v".to_string(), &LogicalType::Integer, 0).format_code,
            match FormatCode::Text {
                FormatCode::Text => 0,
                FormatCode::Binary => 1,
            }
        );
    }
}
