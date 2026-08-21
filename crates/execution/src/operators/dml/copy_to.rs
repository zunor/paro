// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::physical::specs::CopyToFileSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{CopyToSinkGlobal, CopyToSinkLocal, SinkGlobal, SinkLocal};

use super::helpers::{
    build_per_thread_output_path, copy_to_global, copy_to_local, dml_result_chunk,
};

#[derive(Debug, Clone)]
pub struct CopyToFileSinkExec {
    pub spec: CopyToFileSpec,
}

impl CopyToFileSinkExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let global_state = if self.spec.per_thread_output {
            None
        } else {
            Some(Mutex::new((self
                .spec
                .copy_function
                .copy_to_initialize_global)(
                &*self.spec.bind_data,
                &self.spec.file_path,
            )?))
        };
        Ok(SinkGlobal::CopyToFile(Arc::new(CopyToSinkGlobal {
            row_count: AtomicU64::new(0),
            per_thread_output: self.spec.per_thread_output,
            global_state,
            next_file_id: AtomicUsize::new(0),
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        copy_to_global(global)?;
        let local_state =
            (self.spec.copy_function.copy_to_initialize_local)(&*self.spec.bind_data)?;
        Ok(SinkLocal::CopyToFile(CopyToSinkLocal {
            local_state,
            thread_global_state: None,
        }))
    }

    pub(crate) fn consume(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let global = copy_to_global(global)?;
        let local = copy_to_local(local)?;
        write_copy_to_chunk(&self.spec, global, local, input)?;
        global
            .row_count
            .fetch_add(input.size() as u64, Ordering::SeqCst);
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let global = copy_to_global(global)?;
        let local = copy_to_local(local)?;
        if self.spec.per_thread_output {
            ensure_copy_thread_global(&self.spec, global, local)?;
            if let Some(thread_global_state) = local.thread_global_state.as_mut() {
                (self.spec.copy_function.copy_to_combine)(
                    &*self.spec.bind_data,
                    &mut **thread_global_state,
                    &mut *local.local_state,
                )?;
                (self.spec.copy_function.copy_to_finalize)(
                    &*self.spec.bind_data,
                    &mut **thread_global_state,
                )?;
            }
        } else {
            let global_lock = global
                .global_state
                .as_ref()
                .ok_or_else(|| paro_error::internal("missing COPY TO global sink state"))?;
            let mut global_state = global_lock
                .lock()
                .map_err(|error| paro_error::internal(error.to_string()))?;
            (self.spec.copy_function.copy_to_combine)(
                &*self.spec.bind_data,
                &mut **global_state,
                &mut *local.local_state,
            )?;
        }
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<FinishWork> {
        Ok(FinishWork::None)
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let global = copy_to_global(global)?;
        if self.spec.per_thread_output {
            finalize_empty_thread_outputs(
                &self.spec,
                global,
                ctx.query.session.number_of_threads().max(1),
            )?;
        } else {
            let global_lock = global
                .global_state
                .as_ref()
                .ok_or_else(|| paro_error::internal("missing COPY TO global sink state"))?;
            let mut global_state = global_lock
                .lock()
                .map_err(|error| paro_error::internal(error.to_string()))?;
            (self.spec.copy_function.copy_to_finalize)(&*self.spec.bind_data, &mut **global_state)?;
        }
        Ok(FinishPoll::DoneWithResult(dml_result_chunk(
            ctx,
            global.row_count.load(Ordering::SeqCst),
        )?))
    }
}

fn finalize_empty_thread_outputs(
    spec: &CopyToFileSpec,
    global: &CopyToSinkGlobal,
    thread_count: usize,
) -> Result<()> {
    loop {
        let file_id = global.next_file_id.load(Ordering::SeqCst);
        if file_id >= thread_count {
            return Ok(());
        }
        if global
            .next_file_id
            .compare_exchange(file_id, file_id + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            continue;
        }
        let thread_path = build_per_thread_output_path(&spec.file_path, file_id);
        let mut state =
            (spec.copy_function.copy_to_initialize_global)(&*spec.bind_data, &thread_path)?;
        (spec.copy_function.copy_to_finalize)(&*spec.bind_data, &mut *state)?;
    }
}

fn ensure_copy_thread_global(
    spec: &CopyToFileSpec,
    global: &CopyToSinkGlobal,
    local: &mut CopyToSinkLocal,
) -> Result<()> {
    if local.thread_global_state.is_some() {
        return Ok(());
    }
    let file_id = global.next_file_id.fetch_add(1, Ordering::SeqCst);
    let thread_path = build_per_thread_output_path(&spec.file_path, file_id);
    let state = (spec.copy_function.copy_to_initialize_global)(&*spec.bind_data, &thread_path)?;
    local.thread_global_state = Some(state);
    Ok(())
}

fn write_copy_to_chunk(
    spec: &CopyToFileSpec,
    global: &CopyToSinkGlobal,
    local: &mut CopyToSinkLocal,
    chunk: &Chunk,
) -> Result<()> {
    if spec.per_thread_output {
        ensure_copy_thread_global(spec, global, local)?;
        let thread_global_state = local
            .thread_global_state
            .as_mut()
            .ok_or_else(|| paro_error::internal("missing COPY TO per-thread state"))?;
        (spec.copy_function.copy_to_sink)(
            &*spec.bind_data,
            &mut **thread_global_state,
            &mut *local.local_state,
            chunk,
        )
    } else {
        let global_lock = global
            .global_state
            .as_ref()
            .ok_or_else(|| paro_error::internal("missing COPY TO global state"))?;
        let mut global_state = global_lock
            .lock()
            .map_err(|error| paro_error::internal(error.to_string()))?;
        (spec.copy_function.copy_to_sink)(
            &*spec.bind_data,
            &mut **global_state,
            &mut *local.local_state,
            chunk,
        )
    }
}
