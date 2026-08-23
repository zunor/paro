// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use async_trait::async_trait;
use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_instance::Instance;
use paro_session::{
    CollectingSink, CopyInSpec, CopyProtocolSink, CopyProtocolSource, ProtocolResultSink,
    ResultSink, Session, StatementCompletion,
};
use query_i64_col::query_i64_col;
use std::collections::VecDeque;
use std::time::Duration;
use tokio_util::bytes::Bytes;
use unique_test_dir::create_unique_test_dir;

fn create_test_instance(prefix: &str) -> std::sync::Arc<Instance> {
    let dir = create_unique_test_dir("frontend_protocol", prefix);
    create_persistent_instance(&dir)
}

#[derive(Default)]
struct MockCopyOutSink {
    names: Vec<String>,
    rows: usize,
    completion: Option<StatementCompletion>,
    errors: Vec<String>,
}

#[async_trait]
impl ResultSink for MockCopyOutSink {
    async fn start_result(&mut self, _names: &[String], _types: &[LogicalType]) -> Result<()> {
        panic!("COPY TO STDOUT should use CopyProtocolSink, not ResultSink::start_result");
    }

    async fn push_chunk(&mut self, _chunk: &Chunk) -> Result<()> {
        panic!("COPY TO STDOUT should use CopyProtocolSink, not ResultSink::push_chunk");
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.completion = Some(completion.clone());
        Ok(())
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.errors.push(err.to_string());
        Ok(())
    }
}

impl ProtocolResultSink for MockCopyOutSink {
    fn create_copy_out_sink(
        &mut self,
        _cancellation: &paro_session::StatementCancellation,
        _options: &paro_function::copy::CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        Ok(Box::new(MockCopyOutProtocol { sink: self }))
    }
}

struct MockCopyOutProtocol<'a> {
    sink: &'a mut MockCopyOutSink,
}

#[async_trait]
impl CopyProtocolSink for MockCopyOutProtocol<'_> {
    async fn start_copy_out(&mut self, names: &[String], _types: &[LogicalType]) -> Result<()> {
        self.sink.names = names.to_vec();
        Ok(())
    }

    async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()> {
        self.sink.rows += chunk.len();
        Ok(())
    }

    async fn finish_copy_out(&mut self) -> Result<()> {
        Ok(())
    }
}

struct MockCopyInSink {
    payloads: VecDeque<Vec<u8>>,
    column_count: Option<usize>,
    completion: Option<StatementCompletion>,
    errors: Vec<String>,
}

impl MockCopyInSink {
    fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payloads: VecDeque::from([payload.into()]),
            column_count: None,
            completion: None,
            errors: Vec::new(),
        }
    }

    fn from_chunks(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            payloads: chunks.into_iter().collect(),
            column_count: None,
            completion: None,
            errors: Vec::new(),
        }
    }
}

#[async_trait]
impl ResultSink for MockCopyInSink {
    async fn start_result(&mut self, _names: &[String], _types: &[LogicalType]) -> Result<()> {
        panic!("COPY FROM STDIN should not emit row results");
    }

    async fn push_chunk(&mut self, _chunk: &Chunk) -> Result<()> {
        panic!("COPY FROM STDIN should not emit row chunks");
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.completion = Some(completion.clone());
        Ok(())
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.errors.push(err.to_string());
        Ok(())
    }
}

impl ProtocolResultSink for MockCopyInSink {
    fn create_copy_in_source(
        &mut self,
        _cancellation: &paro_session::StatementCancellation,
    ) -> Result<Box<dyn CopyProtocolSource + '_>> {
        Ok(Box::new(MockCopyInProtocol { sink: self }))
    }
}

struct MockCopyInProtocol<'a> {
    sink: &'a mut MockCopyInSink,
}

#[async_trait]
impl CopyProtocolSource for MockCopyInProtocol<'_> {
    async fn begin_copy_in(&mut self, spec: &CopyInSpec) -> Result<()> {
        self.sink.column_count = Some(spec.column_formats.len());
        Ok(())
    }

    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        Ok(self.sink.payloads.pop_front().map(Bytes::from))
    }
}

fn binary_vector_copy_payload(rows: &[(i32, [f32; 3])]) -> Vec<u8> {
    let mut payload = b"PGCOPY\n\xff\r\n\0".to_vec();
    payload.extend_from_slice(&0_u32.to_be_bytes());
    payload.extend_from_slice(&0_u32.to_be_bytes());
    let float_oid = LogicalType::Float.pg_descriptor().oid;
    for (id, vector) in rows {
        payload.extend_from_slice(&2_i16.to_be_bytes());
        payload.extend_from_slice(&4_i32.to_be_bytes());
        payload.extend_from_slice(&id.to_be_bytes());

        let vector_bytes = 20 + vector.len() * 8;
        payload.extend_from_slice(&(vector_bytes as i32).to_be_bytes());
        payload.extend_from_slice(&1_i32.to_be_bytes());
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&float_oid.to_be_bytes());
        payload.extend_from_slice(&(vector.len() as i32).to_be_bytes());
        payload.extend_from_slice(&1_i32.to_be_bytes());
        for value in vector {
            payload.extend_from_slice(&4_i32.to_be_bytes());
            payload.extend_from_slice(&value.to_be_bytes());
        }
    }
    payload.extend_from_slice(&(-1_i16).to_be_bytes());
    payload
}

#[tokio::test]
async fn explain_execute_uses_session_prepared_cache() {
    let instance = create_test_instance("prepared_cache");
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "PREPARE stmt1 AS SELECT 1").await;

    exec_ok(&mut session, &mut sink, "EXPLAIN EXECUTE stmt1").await;

    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Explain);
    assert!(
        !result.chunks.is_empty(),
        "EXPLAIN should return output rows"
    );
}

#[tokio::test]
async fn copy_to_stdout_uses_copy_protocol_sink() {
    let instance = create_test_instance("copy_out");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut collect,
        "CREATE TABLE copy_out_t (v INT)",
    )
    .await;
    let mut load_sink = MockCopyInSink::new("1\n2\n");
    let load_result = session
        .execute_simple_query(
            "COPY copy_out_t (v) FROM STDIN WITH (FORMAT csv)",
            &mut load_sink,
        )
        .await;
    assert!(
        load_result.is_ok(),
        "COPY FROM STDIN should succeed during setup: {load_result:?}"
    );
    assert!(
        load_sink.errors.is_empty(),
        "unexpected setup sink errors: {:?}",
        load_sink.errors
    );

    let mut sink = MockCopyOutSink::default();
    let result = session
        .execute_simple_query(
            "COPY copy_out_t TO STDOUT WITH (FORMAT csv, HEADER true)",
            &mut sink,
        )
        .await;

    assert!(result.is_ok(), "COPY TO STDOUT should succeed: {result:?}");
    assert!(
        sink.errors.is_empty(),
        "unexpected sink errors: {:?}",
        sink.errors
    );
    assert_eq!(sink.names, vec!["v".to_string()]);
    assert_eq!(sink.rows, 2);
    assert_eq!(sink.completion, Some(StatementCompletion::Copy { rows: 2 }));
}

#[tokio::test]
async fn copy_from_stdin_uses_copy_protocol_source() {
    let instance = create_test_instance("copy_in");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();

    exec_ok(&mut session, &mut collect, "CREATE TABLE copy_in_t (v INT)").await;

    let mut sink = MockCopyInSink::new("1\n2\n");
    let result = session
        .execute_simple_query("COPY copy_in_t (v) FROM STDIN WITH (FORMAT csv)", &mut sink)
        .await;

    assert!(result.is_ok(), "COPY FROM STDIN should succeed: {result:?}");
    assert!(
        sink.errors.is_empty(),
        "unexpected sink errors: {:?}",
        sink.errors
    );
    assert_eq!(sink.column_count, Some(1));
    assert_eq!(sink.completion, Some(StatementCompletion::Copy { rows: 2 }));

    exec_ok(
        &mut session,
        &mut collect,
        "SELECT v FROM copy_in_t ORDER BY v",
    )
    .await;
    assert_eq!(query_i64_col(&collect, 0), vec![1, 2]);
}

#[tokio::test]
async fn binary_copy_from_stdin_decodes_vector_columns_end_to_end() {
    let instance = create_test_instance("copy_in_binary_vector");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();
    exec_ok(
        &mut session,
        &mut collect,
        "CREATE TABLE copy_in_binary_vector (id INT, embedding VECTOR(3))",
    )
    .await;

    let mut sink = MockCopyInSink::new(binary_vector_copy_payload(&[
        (7, [1.0, 2.0, 3.0]),
        (9, [4.0, 5.0, 6.0]),
    ]));
    session
        .execute_simple_query(
            "COPY copy_in_binary_vector FROM STDIN WITH (FORMAT binary)",
            &mut sink,
        )
        .await
        .expect("binary vector COPY");
    assert_eq!(sink.completion, Some(StatementCompletion::Copy { rows: 2 }));

    exec_ok(
        &mut session,
        &mut collect,
        "SELECT id FROM copy_in_binary_vector ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&collect, 0), vec![7, 9]);
}

#[tokio::test]
async fn copy_from_stdin_decode_error_closes_stream_before_joining_worker() {
    let instance = create_test_instance("copy_in_early_error");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();
    exec_ok(
        &mut session,
        &mut collect,
        "CREATE TABLE copy_in_early_error (v INT)",
    )
    .await;

    let chunks =
        std::iter::once(b"not-an-integer\n".to_vec()).chain((0..128).map(|_| b"1\n".to_vec()));
    let mut sink = MockCopyInSink::from_chunks(chunks);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        session.execute_simple_query(
            "COPY copy_in_early_error FROM STDIN WITH (FORMAT csv)",
            &mut sink,
        ),
    )
    .await
    .expect("COPY error cleanup must not deadlock")
    .expect_err("invalid integer should fail COPY");

    assert!(result.to_string().contains("not-an-integer"));
    assert_eq!(sink.errors.len(), 1);
}

#[tokio::test]
async fn copy_from_stdin_defaults_escape_to_custom_quote() {
    let instance = create_test_instance("copy_in_custom_quote");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut collect,
        "CREATE TABLE copy_in_custom_quote (id INT, name VARCHAR)",
    )
    .await;

    let mut sink = MockCopyInSink::new("1,'it''s'\n");
    let result = session
        .execute_simple_query(
            "COPY copy_in_custom_quote FROM STDIN WITH (FORMAT csv, QUOTE '''')",
            &mut sink,
        )
        .await;

    assert!(
        result.is_ok(),
        "custom QUOTE COPY should succeed: {result:?}"
    );
    assert!(
        sink.errors.is_empty(),
        "unexpected errors: {:?}",
        sink.errors
    );
    assert_eq!(sink.completion, Some(StatementCompletion::Copy { rows: 1 }));

    exec_ok(
        &mut session,
        &mut collect,
        "SELECT id FROM copy_in_custom_quote WHERE name = 'it''s'",
    )
    .await;
    assert_eq!(query_i64_col(&collect, 0), vec![1]);
}

#[tokio::test]
async fn copy_from_stdin_routes_ndjson_through_statement_input() {
    let instance = create_test_instance("copy_in_ndjson");
    let mut session = Session::new(1, instance);
    let mut collect = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut collect,
        "CREATE TABLE copy_in_json (id INT, name VARCHAR)",
    )
    .await;

    let mut sink =
        MockCopyInSink::new("{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n");
    let result = session
        .execute_simple_query(
            "COPY copy_in_json FROM STDIN WITH (FORMAT ndjson)",
            &mut sink,
        )
        .await;

    assert!(result.is_ok(), "NDJSON COPY should succeed: {result:?}");
    assert!(
        sink.errors.is_empty(),
        "unexpected errors: {:?}",
        sink.errors
    );
    assert_eq!(sink.column_count, Some(2));
    assert_eq!(sink.completion, Some(StatementCompletion::Copy { rows: 2 }));

    exec_ok(
        &mut session,
        &mut collect,
        "SELECT id FROM copy_in_json ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&collect, 0), vec![1, 2]);
}
