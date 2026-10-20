// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Synchronous engine facade over an owned asynchronous Flight runtime.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc, Mutex, Weak,
};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_client::FlightServiceClient,
    Action, FlightDescriptor,
};
use arrow_schema::Schema;
use futures::{stream, TryStreamExt};
use paro_common::allocator::Allocator;
use paro_common::error::{self as error, Result};
use paro_common::vector::Vector;
use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use super::{
    codec, definition::Definition, process::Service, MAX_BATCH_BYTES, MAX_BATCH_ROWS,
    MAX_CONTROL_BYTES,
};

#[derive(Clone)]
struct Connection {
    channel: Channel,
    token: String,
    epoch: String,
    slots: Arc<Semaphore>,
    owners: Arc<Mutex<HashMap<String, Weak<Semaphore>>>>,
    sequence: Arc<AtomicU64>,
    shutdown: CancellationToken,
    stopped: CancellationToken,
}

pub struct PythonClient {
    connection: Connection,
    runtime: Handle,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for PythonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonClient")
            .field("epoch", &self.connection.epoch)
            .finish_non_exhaustive()
    }
}

fn failure(message: impl std::fmt::Display) -> paro_common::error::ParoError {
    let message = message.to_string();
    // Keep driver diagnostics out of SQL errors; preserve the typed worker cause.
    for (marker, constructor) in [
        (
            "TYPE_CONTRACT:",
            error::contract_violation as fn(String) -> _,
        ),
        (
            "RESOURCE_EXHAUSTED:",
            error::contract_violation as fn(String) -> _,
        ),
        (
            "PYTHON_EXCEPTION:",
            error::python_exception as fn(String) -> _,
        ),
        (
            "CANCELLED:",
            error::query_canceled_message as fn(String) -> _,
        ),
        (
            "PROTOCOL:",
            error::external_protocol_mismatch as fn(String) -> _,
        ),
    ] {
        if let Some((_, detail)) = message.split_once(marker) {
            let detail = detail
                .split(". Detail:")
                .next()
                .unwrap_or(detail)
                .lines()
                .next()
                .unwrap_or(detail)
                .trim();
            return constructor(detail.to_string());
        }
    }
    error::worker_failure(message)
}

/// Check IPC declarations before the decoder can allocate/decompress buffers.
fn validate_frame(data: &arrow_flight::FlightData) -> std::result::Result<(), &'static str> {
    let invalid = || "TYPE_CONTRACT: Python Arrow frame exceeds wire contract";
    if data.data_header.len() > MAX_CONTROL_BYTES
        || data.app_metadata.len() > MAX_CONTROL_BYTES
        || data.data_body.len() > MAX_BATCH_BYTES
    {
        return Err(invalid());
    }
    if data.data_header.is_empty() {
        return if data.data_body.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    let message = arrow_ipc::root_as_message(&data.data_header).map_err(|_| invalid())?;
    if message.bodyLength() != data.data_body.len() as i64 {
        return Err(invalid());
    }
    match message.header_type() {
        arrow_ipc::MessageHeader::Schema | arrow_ipc::MessageHeader::NONE
            if data.data_body.is_empty() =>
        {
            Ok(())
        }
        arrow_ipc::MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or_else(invalid)?;
            if batch.compression().is_some()
                || !(0..=MAX_BATCH_ROWS as i64).contains(&batch.length())
            {
                return Err(invalid());
            }
            let mut elements = MAX_BATCH_BYTES / size_of::<paro_common::runtime_value::Value>();
            for node in batch.nodes().ok_or_else(invalid)? {
                let length = usize::try_from(node.length()).map_err(|_| invalid())?;
                elements = elements.checked_sub(length).ok_or_else(invalid)?;
                if node.null_count() < 0 || node.null_count() > node.length() {
                    return Err(invalid());
                }
            }
            for buffer in batch.buffers().ok_or_else(invalid)? {
                if buffer.offset() < 0
                    || buffer.length() < 0
                    || buffer
                        .offset()
                        .checked_add(buffer.length())
                        .is_none_or(|end| end > message.bodyLength())
                {
                    return Err(invalid());
                }
            }
            Ok(())
        }
        _ => Err(invalid()),
    }
}

impl PythonClient {
    pub fn is_running(&self) -> bool {
        !self.connection.shutdown.is_cancelled()
            && self
                .thread
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|thread| !thread.is_finished())
    }
    pub fn start() -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let shutdown = CancellationToken::new();
        let service_shutdown = shutdown.clone();
        let stopped = CancellationToken::new();
        let service_stopped = stopped.clone();
        let thread = std::thread::Builder::new()
            .name("paro-python-flight".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("paro-python-io")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready_tx.send(Err(failure(e)));
                        return;
                    }
                };
                runtime.block_on(async {
                    let mut service = match Service::start().await {
                        Ok(service) => service,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    };
                    let connection = tokio::time::timeout(Duration::from_secs(10), async {
                        let channel =
                            Channel::from_shared(format!("http://127.0.0.1:{}", service.port))
                                .map_err(failure)?
                                .connect_timeout(Duration::from_secs(5))
                                .connect()
                                .await
                                .map_err(failure)?;
                        let connection = Connection {
                            channel,
                            token: service.token.clone(),
                            epoch: service.epoch.clone(),
                            slots: Arc::new(Semaphore::new(8)),
                            owners: Arc::new(Mutex::new(HashMap::new())),
                            sequence: Arc::new(AtomicU64::new(1)),
                            shutdown: service_shutdown.clone(),
                            stopped: service_stopped.clone(),
                        };
                        let capabilities = connection.action("capabilities", json!({})).await?;
                        if capabilities["version"] != 1
                            || capabilities["window"] != 1
                            || capabilities["max_batch_bytes"] != MAX_BATCH_BYTES
                            || capabilities["max_batch_rows"] != MAX_BATCH_ROWS
                            || capabilities["max_handlers"] != 8
                            || capabilities["max_owner_handlers"] != 4
                        {
                            return Err(error::external_protocol_mismatch(
                                "Python worker capability mismatch",
                            ));
                        }
                        Ok(connection)
                    })
                    .await
                    .map_err(failure)
                    .and_then(|result| result);
                    let started = connection.is_ok();
                    let _ = ready_tx
                        .send(connection.map(|connection| (connection, runtime.handle().clone())));
                    if started {
                        tokio::select! {
                            _ = service_shutdown.cancelled() => {},
                            _ = service.child.wait() => {},
                        }
                    }
                    service_shutdown.cancel();
                    service.stop().await;
                });
                service_stopped.cancel();
                runtime.shutdown_timeout(Duration::from_secs(5));
            })?;
        match ready_rx.recv_timeout(Duration::from_secs(25)) {
            Ok(Ok((connection, runtime))) => Ok(Self {
                connection,
                runtime,
                thread: Mutex::new(Some(thread)),
            }),
            result => {
                shutdown.cancel();
                let _ = thread.join();
                match result {
                    Ok(Err(e)) => Err(e),
                    _ => Err(failure("Python service startup timed out")),
                }
            }
        }
    }

    fn wait<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T>> + Send + 'static,
        cancel: impl Fn() -> Result<()>,
        token: CancellationToken,
    ) -> Result<T> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            let _ = tx.send(work.await);
        });
        let wait = || {
            let mut cancelled = None;
            loop {
                match rx.recv_timeout(Duration::from_millis(20)) {
                    Ok(result) => {
                        return match cancelled {
                            Some(e) => Err(e),
                            None => result,
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(cancelled.unwrap_or_else(|| failure("Python service stopped")))
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if cancelled.is_none() {
                            if let Err(e) = cancel() {
                                token.cancel();
                                cancelled = Some(e);
                            }
                        }
                    }
                }
            }
        };
        // Fetch-driven SQL can call this synchronous facade on a Tokio worker.
        // Hand that worker back before waiting, otherwise pgwire CancelRequest
        // and unrelated socket I/O may not be polled until the UDF completes.
        if Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        }) {
            tokio::task::block_in_place(wait)
        } else {
            wait()
        }
    }

    pub fn validate(&self, definition: Definition) -> Result<()> {
        let connection = self.connection.clone();
        self.wait(
            async move {
                tokio::time::timeout(Duration::from_secs(15), async {
                    let _permits = connection.acquire(&definition.owner).await?;
                    connection
                        .action("validate", json!({"definition": definition}))
                        .await
                })
                .await
                .map_err(failure)??;
                Ok(())
            },
            || Ok(()),
            CancellationToken::new(),
        )
    }

    pub fn execute(
        &self,
        definition: Definition,
        input: RecordBatch,
        output: Arc<Schema>,
        allocator: Arc<dyn Allocator>,
        cancel: impl Fn() -> Result<()>,
    ) -> Result<Vec<Vector>> {
        cancel()?;
        if input.num_rows() > MAX_BATCH_ROWS || input.get_array_memory_size() > MAX_BATCH_BYTES {
            return Err(error::contract_violation(
                "Python input exceeds Arrow batch budget",
            ));
        }
        let connection = self.connection.clone();
        let fence = definition.fence();
        let token = CancellationToken::new();
        let task_token = token.clone();
        self.wait(async move {
            let _permits = tokio::select! {
                permits = connection.acquire(&definition.owner) => permits?,
                _ = task_token.cancelled() => return Err(error::query_canceled()),
                _ = connection.shutdown.cancelled() => return Err(failure("Python service stopped")),
                _ = tokio::time::sleep(Duration::from_secs(30)) => return Err(failure("Python admission deadline exceeded")),
            };
            // Allocate only after admission: queued owners must not consume or
            // age out the bounded server replay window.
            if task_token.is_cancelled() {
                return Err(error::query_canceled());
            }
            if connection.shutdown.is_cancelled() {
                return Err(failure("Python service stopped"));
            }
            let ordinal = connection.sequence.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
                |value| value.checked_add(1)).map_err(|_| failure("Python invocation ordinal exhausted"))?;
            let id = format!("{}-{ordinal}", connection.epoch);
            let outcome = {
                let work = connection.exchange(&id, definition, input, output, allocator);
                tokio::select! {
                    result = work => result,
                    _ = task_token.cancelled() => Err(error::query_canceled()),
                    _ = connection.shutdown.cancelled() => Err(failure("Python service stopped")),
                    _ = tokio::time::sleep(Duration::from_secs(30)) => Err(failure("Python invocation deadline exceeded")),
                }
            };
            if outcome.is_err() {
                // Never replay a request after opening an exchange.
                let cleanup = tokio::time::timeout(Duration::from_secs(15),
                    connection.action("cancel", json!({"id": id, "fence": fence}))).await;
                if !matches!(cleanup, Ok(Ok(_))) {
                    // A missing cleanup acknowledgement is not free capacity.
                    // Retire the epoch and wait for its service owner to stop.
                    connection.shutdown.cancel();
                    connection.stopped.cancelled().await;
                }
            }
            outcome
        }, cancel, token)
    }
}

impl Drop for PythonClient {
    fn drop(&mut self) {
        self.connection.shutdown.cancel();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}

impl Connection {
    async fn acquire(&self, owner: &str) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let owner = self
            .owner_gate(owner)?
            .acquire_owned()
            .await
            .map_err(failure)?;
        let global = self.slots.clone().acquire_owned().await.map_err(failure)?;
        Ok((owner, global))
    }

    fn owner_gate(&self, owner: &str) -> Result<Arc<Semaphore>> {
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| failure("owner admission lock poisoned"))?;
        owners.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = owners.get(owner).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(Semaphore::new(4));
        owners.insert(owner.to_string(), Arc::downgrade(&gate));
        Ok(gate)
    }

    fn control(&self, mut value: Value) -> Result<Vec<u8>> {
        value["version"] = json!(1);
        value["token"] = json!(self.token);
        value["epoch"] = json!(self.epoch);
        let bytes = serde_json::to_vec(&value).map_err(failure)?;
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(error::contract_violation("Python control frame too large"));
        }
        Ok(bytes)
    }

    fn client(&self) -> FlightServiceClient<Channel> {
        FlightServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_BATCH_BYTES + MAX_CONTROL_BYTES)
            .max_encoding_message_size(MAX_BATCH_BYTES + MAX_CONTROL_BYTES)
    }

    async fn action(&self, kind: &str, value: Value) -> Result<Value> {
        let mut response = self
            .client()
            .do_action(Action {
                r#type: kind.into(),
                body: self.control(value)?.into(),
            })
            .await
            .map_err(failure)?
            .into_inner();
        let result = response
            .message()
            .await
            .map_err(failure)?
            .ok_or_else(|| failure("missing action response"))?;
        if result.body.len() > MAX_CONTROL_BYTES {
            return Err(error::external_protocol_mismatch(
                "action response too large",
            ));
        }
        let value: Value = serde_json::from_slice(&result.body).map_err(failure)?;
        if value["version"] != 1 || (kind != "capabilities" && value["ok"] != true) {
            return Err(error::external_protocol_mismatch("invalid action version"));
        }
        if response.message().await.map_err(failure)?.is_some() {
            return Err(failure("extra action response"));
        }
        Ok(value)
    }

    async fn exchange(
        &self,
        id: &str,
        definition: Definition,
        input: RecordBatch,
        output: Arc<Schema>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Vec<Vector>> {
        let fence = definition.fence();
        let expected_rows = input.num_rows();
        let table = definition.mode == "table";
        let descriptor = FlightDescriptor::new_cmd(self.control(json!({
            "id": id, "fence": fence, "definition": definition,
        }))?);
        let mut encoded = FlightDataEncoderBuilder::new()
            .with_max_flight_data_size(MAX_BATCH_BYTES * 2)
            .with_schema(input.schema())
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter(vec![Ok(input)]))
            .try_collect::<Vec<_>>()
            .await
            .map_err(failure)?;
        for message in &mut encoded {
            if arrow_ipc::root_as_message(&message.data_header)
                .map_err(failure)?
                .header_type()
                == arrow_ipc::MessageHeader::RecordBatch
            {
                message.app_metadata = serde_json::to_vec(&json!({"version": 1, "sequence": 0}))
                    .map_err(failure)?
                    .into();
            }
        }
        let mut request = tonic::Request::new(stream::iter(encoded));
        request.set_timeout(Duration::from_secs(30));
        let response = self
            .client()
            .do_exchange(request)
            .await
            .map_err(failure)?
            .into_inner();
        // PyArrow emits metadata-only Flight messages with an empty IPC header.
        // Arrow Rust 53 expects an explicit NONE message for this legal form.
        let response = response
            .map_err(FlightError::Tonic)
            .and_then(|mut data| async move {
                validate_frame(&data).map_err(FlightError::protocol)?;
                if data.data_header.is_empty() {
                    let mut builder = flatbuffers::FlatBufferBuilder::new();
                    let message = arrow_ipc::Message::create(
                        &mut builder,
                        &arrow_ipc::MessageArgs::default(),
                    );
                    builder.finish(message, None);
                    data.data_header = builder.finished_data().to_vec().into();
                }
                Ok(data)
            });
        let mut decoder = FlightDataDecoder::new(response);
        let mut vectors = None;
        let mut schema_seen = false;
        let mut consumed = false;
        let mut finished = false;
        while let Some(message) = decoder.try_next().await.map_err(failure)? {
            let metadata = if message.inner.app_metadata.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice::<Value>(&message.inner.app_metadata).map_err(failure)?
            };
            if !metadata.is_null() && metadata["version"] != 1 {
                return Err(error::external_protocol_mismatch(
                    "invalid result metadata version",
                ));
            }
            match message.payload {
                DecodedPayload::Schema(schema)
                    if schema == output && !schema_seen && !consumed && !finished =>
                {
                    schema_seen = true;
                }
                DecodedPayload::Schema(_) => {
                    return Err(error::contract_violation("Python output schema mismatch"))
                }
                DecodedPayload::None
                    if metadata["kind"] == "consumed"
                        && metadata["sequence"] == 0
                        && schema_seen
                        && !consumed
                        && !finished =>
                {
                    consumed = true;
                }
                DecodedPayload::RecordBatch(batch) => {
                    if !schema_seen
                        || !consumed
                        || finished
                        || vectors.is_some()
                        || metadata["kind"] != "result"
                        || metadata["sequence"] != 0
                        || (!table && batch.num_rows() != expected_rows)
                        || batch.num_rows() > MAX_BATCH_ROWS
                        || batch.get_array_memory_size() > MAX_BATCH_BYTES
                    {
                        return Err(error::contract_violation(
                            "invalid Python result sequence, shape or size",
                        ));
                    }
                    // Materialize and validate into engine-owned memory before releasing the worker slot.
                    vectors = Some(codec::decode(&batch, &output, allocator.clone())?);
                    self.action("ack", json!({"id": id, "fence": fence, "sequence": 0}))
                        .await?;
                }
                DecodedPayload::None
                    if metadata["kind"] == "finish"
                        && metadata["sequence"] == 1
                        && vectors.is_some()
                        && !finished =>
                {
                    self.action("finish", json!({"id": id, "fence": fence}))
                        .await?;
                    finished = true;
                }
                _ => {
                    return Err(error::external_protocol_mismatch(
                        "unexpected Python Flight message",
                    ))
                }
            }
        }
        if !finished {
            return Err(failure(
                "Python invocation ended without Finish acknowledgement",
            ));
        }
        vectors.ok_or_else(|| failure("Python invocation produced no result"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(
        rows: i64,
        elements: i64,
        compressed: bool,
        buffer_bytes: i64,
    ) -> arrow_flight::FlightData {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let compression = compressed.then(|| {
            arrow_ipc::BodyCompression::create(
                &mut builder,
                &arrow_ipc::BodyCompressionArgs::default(),
            )
        });
        let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(elements, elements)]);
        let buffers = builder.create_vector(&[arrow_ipc::Buffer::new(0, buffer_bytes)]);
        let batch = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: rows,
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression,
                ..Default::default()
            },
        );
        let message = arrow_ipc::Message::create(
            &mut builder,
            &arrow_ipc::MessageArgs {
                header_type: arrow_ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                ..Default::default()
            },
        );
        builder.finish(message, None);
        arrow_flight::FlightData {
            data_header: builder.finished_data().to_vec().into(),
            ..Default::default()
        }
    }

    #[test]
    fn wire_limits_are_checked_before_arrow_decoding() {
        assert!(validate_frame(&frame(0, 0, false, 0)).is_ok());
        assert!(validate_frame(&frame(1, 1, true, 0)).is_err());
        assert!(validate_frame(&frame(MAX_BATCH_ROWS as i64 + 1, 0, false, 0)).is_err());
        assert!(validate_frame(&frame(1, MAX_BATCH_BYTES as i64, false, 0)).is_err());
        assert!(validate_frame(&frame(0, 0, false, 1)).is_err());
    }
}
