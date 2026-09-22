// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Render only the sealed target observation; never invoke planning or admission.
use paro_context::compile_diagnostics::{
    CompileRecord, ExecutionReceipt, SealedCompileCapture, ENCODED_LIMIT,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

struct LimitedWriter(Vec<u8>);
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > ENCODED_LIMIT.saturating_sub(self.0.len()) {
            return Err(io::Error::other("compile document capacity"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_json_bounded<T: serde::Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let mut writer = LimitedWriter(Vec::new());
    writer
        .0
        .try_reserve_exact(ENCODED_LIMIT)
        .map_err(|_| io::Error::other("compile document capacity"))?;
    serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
    Ok(writer.0)
}

pub fn render(capture: &SealedCompileCapture, json: bool) -> String {
    render_with_execution(capture, json, None)
}

/// Render a sealed compile record and, when ANALYZE actually ran the same
/// compiled statement, append the immutable execution receipt.  Admission is
/// never inferred from the portfolio fields in the compile record.
pub fn render_with_execution(
    capture: &SealedCompileCapture,
    json: bool,
    execution: Option<ExecutionReceipt>,
) -> String {
    render_with_execution_level(capture, json, execution, false)
}

pub fn render_with_execution_level(
    capture: &SealedCompileCapture,
    json: bool,
    execution: Option<ExecutionReceipt>,
    include_detail: bool,
) -> String {
    let mut writer = LimitedWriter(Vec::new());
    if writer.0.try_reserve_exact(ENCODED_LIMIT).is_err() {
        return unavailable(json);
    }
    let result = capture.read(|record| {
        let mut record: CompileRecord = record.clone();
        record.execution_receipt = execution;
        if json {
            // Detail is optional; the summary, terminal state, and receipt
            // are not.  Trim only optional events when the sealed document is
            // larger than the wire lease, instead of replacing an executed
            // ANALYZE result with an Unavailable/NotExecuted document.
            let mut encoded = encode_json_bounded(&record);
            if encoded.is_err() && !record.detail.is_empty() {
                record.omitted_encoding_detail = record
                    .omitted_encoding_detail
                    .saturating_add(record.detail.len() as u64);
                record.detail.clear();
                encoded = encode_json_bounded(&record);
            }
            writer.write_all(&encoded?)
        } else {
            writeln!(writer, "EXPLAIN (COMPILE) / schema {} / ForcedCompile", record.schema_version)?;
            writeln!(writer, "phase             nanoseconds (Observed / Uncovered)")?;
            writeln!(writer, "bind              {:?}\noptimizer         {:?}\nverify            {:?}\nfinish            {:?}\ncompiler wall     {:?}", record.bind_ns, record.optimizer_ns, record.verify_ns, record.finish_ns, record.compiler_ns)?;
            writeln!(writer, "compiler other    {:?}\nparse             {:?}", record.compiler_other_ns, record.parse)?;
            writeln!(writer, "artifact={:?} safety={:?} stop={:?} complete={:?} obligations={:?}", record.artifact, record.safety_verified, record.search_stop, record.search_complete, record.obligations)?;
            writeln!(writer, "quality_satisfied={:?} budget_limited={:?} groups={:?} logical={:?} physical={:?}", record.quality_policy_satisfied, record.budget_limited, record.groups, record.logical_expressions, record.physical_expressions)?;
            writeln!(writer, "input={:?} output={:?} artifact_identity={:?} settings={:?} memory_bytes={:?} parallel_tasks={:?}", record.input_fingerprint, record.output_identity, record.artifact_identity, record.planning_settings, record.available_memory_bytes, record.available_parallel_tasks)?;
            writeln!(writer, "expected_class={:?} variants={:?} selected_fingerprint={:?}", record.expected_class, record.variant_count, record.selected_fingerprint)?;
            for v in &record.variants { writeln!(writer, "variant {} fingerprint={:?} admissible_classes={}", v.ordinal,v.physical_fingerprint,v.admissible_classes)?; }
            writeln!(writer, "admission={:?} execution={:?}", record.admission, record.execution)?;
            for r in &record.rules { writeln!(writer, "rule {} binding_calls={} binding_ns={} apply_attempts={} apply_ns={} inserted={} elapsed_ns={}",r.id,r.binding_calls,r.binding_ns,r.attempts,r.elapsed_ns.saturating_sub(r.binding_ns),r.inserted,r.elapsed_ns)?; }
            writeln!(writer, "omitted_rules={} omitted_variants={} omitted_source_detail={} omitted_capture_detail={} omitted_encoding_detail={} retained_limit={} encoded_limit={} response_terminal={:?} execution_receipt={:?}",record.omitted_rules,record.omitted_variants,record.omitted_source_detail,record.omitted_capture_detail,record.omitted_encoding_detail,record.retained_limit,record.encoded_limit,record.response_terminal,record.execution_receipt)?;
            if include_detail {
                for event in &record.detail {
                    writeln!(writer, "detail {}", serde_json::to_string(event).map_err(io::Error::other)?)?;
                }
            }
            Ok(())
        }
    });
    if result.is_err() {
        return unavailable(json);
    }
    String::from_utf8(writer.0).expect("UTF-8 renderer")
}

pub fn unavailable(json: bool) -> String {
    use paro_context::compile_diagnostics::{CompileDocument, UnavailableReason};
    if json {
        serde_json::to_string(&CompileDocument::unavailable(UnavailableReason::Capacity))
            .expect("fixed-size unavailable document")
    } else {
        "EXPLAIN (COMPILE): DiagnosticUnavailable(Capacity)".into()
    }
}

/// Current schema only. Size is checked before deserializing untrusted input.
pub fn validate_json(
    bytes: &[u8],
) -> Result<paro_context::compile_diagnostics::CompileDocument, String> {
    use paro_context::compile_diagnostics::*;
    if bytes.len() > ENCODED_LIMIT {
        return Err("encoded capacity exceeded".into());
    }
    let document: CompileDocument = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let r = match &document {
        CompileDocument::Summary(r) => r,
        CompileDocument::Unavailable(r) => {
            if r.schema_version != SCHEMA_VERSION
                || r.target_compile != CompileOutcome::Success
                || r.target_execution != Observation::NotExecuted
            {
                return Err("inconsistent unavailable document".into());
            }
            return Ok(document);
        }
    };
    if r.schema_version != SCHEMA_VERSION
        || r.encoded_limit != ENCODED_LIMIT
        || r.retained_limit != RETAINED_LIMIT
        || r.process_limit != PROCESS_LIMIT
        || r.process_reservation != 2 << 20
        || r.rules.len() > MAX_RULES
        || r.search_counters.len() > MAX_SEARCH_COUNTERS
        || r.variants.len() > MAX_VARIANTS
        || r.detail_limit != MAX_DETAIL_EVENTS
        || r.detail.len() > MAX_DETAIL_EVENTS
        || (r.capture_level == CaptureLevel::Summary
            && (r.omitted_source_detail != 0
                || r.omitted_capture_detail != 0
                || r.omitted_encoding_detail != 0
                || !r.detail.is_empty()))
    {
        return Err("schema/capacity profile mismatch".into());
    }
    let mut search_counter_names = BTreeSet::new();
    for counter in &r.search_counters {
        if counter.name.is_empty() || !search_counter_names.insert(counter.name.as_str()) {
            return Err("search counter names are not unique".into());
        }
    }
    let mut source_sequences = BTreeMap::<&'static str, u64>::new();
    let candidate_event_ids: BTreeSet<u64> = r
        .detail
        .iter()
        .filter_map(|event| match event {
            DetailEvent::Candidate {
                source_sequence, ..
            } => Some(*source_sequence),
            _ => None,
        })
        .collect();
    let mut child_ordinals = BTreeSet::new();
    let mut fact_ordinals = BTreeSet::new();
    for event in &r.detail {
        let stream = event.stream_name();
        if let Some(previous) = source_sequences.insert(stream, event.source_sequence()) {
            if event.source_sequence() <= previous {
                return Err("detail source sequence is unordered".into());
            }
        }
        match event {
            DetailEvent::CandidateChild {
                parent_event_id,
                ordinal,
                ..
            } => {
                if !candidate_event_ids.contains(parent_event_id) {
                    return Err("candidate child references an unknown parent event".into());
                }
                if !child_ordinals.insert((*parent_event_id, *ordinal)) {
                    return Err("candidate child ordinal is not unique for its parent".into());
                }
            }
            DetailEvent::Fact {
                parent_event_id,
                ordinal,
                ..
            } => {
                if !candidate_event_ids.contains(parent_event_id) {
                    return Err("fact references an unknown parent event".into());
                }
                if !fact_ordinals.insert((*parent_event_id, *ordinal)) {
                    return Err("fact ordinal is not unique for its parent".into());
                }
            }
            _ => {}
        }
    }
    if let Observation::Observed(identity) = r.artifact_identity {
        if identity.schema_version != IDENTITY_SCHEMA_VERSION {
            return Err("compile artifact uses an unsupported identity schema".into());
        }
    }
    if let Some(execution) = &r.execution_receipt {
        use paro_context::compile_diagnostics::{LoweringStatus, ResourceReservationStatus};
        if execution.schema_version != paro_context::compile_diagnostics::RECEIPT_SCHEMA_VERSION
            || r.artifact_identity != Observation::Observed(execution.artifact_identity)
        {
            return Err("execution receipt is not bound to the sealed compile artifact".into());
        }
        if execution.artifact_identity.schema_version
            != paro_context::compile_diagnostics::IDENTITY_SCHEMA_VERSION
        {
            return Err("execution receipt uses an unsupported artifact identity schema".into());
        }
        if execution.admission == AdmissionResult::Selected
            && (execution.actual_class.is_none()
                || execution.actual_fingerprint.is_none()
                || execution.resources.is_none()
                || execution.selection_identity.is_none())
        {
            return Err("selected execution receipt lacks actual admission".into());
        }
        if let (Some(actual_class), Some(resources)) = (execution.actual_class, execution.resources)
        {
            if resources.class != actual_class
                || resources.max_parallel_tasks == 0
                || resources.memory_ceiling_bytes < resources.minimum_memory_bytes
                || resources.working_set_memory_bytes < resources.minimum_memory_bytes
            {
                return Err("execution resource contract is inconsistent".into());
            }
            if let paro_context::compile_diagnostics::MemoryCompletionReceipt::RuntimeCappedKnown {
                uncapped_memory_bytes,
            } = resources.memory_completion
            {
                if uncapped_memory_bytes < resources.working_set_memory_bytes {
                    return Err("known uncapped memory is below the working set".into());
                }
            }
        }
        if execution.admission == AdmissionResult::Selected
            && matches!(
                execution.terminal,
                ExecutionTerminal::Running
                    | ExecutionTerminal::Completed
                    | ExecutionTerminal::Dropped
            )
            && execution.image != ExecutionImageStatus::Ready
        {
            return Err("selected execution terminal lacks an executable image".into());
        }
        if execution.admission != AdmissionResult::Selected
            && (execution.actual_class.is_some()
                || execution.actual_fingerprint.is_some()
                || execution.selection_identity.is_some()
                || execution.resources.is_some()
                || execution.reservation != ResourceReservationStatus::NotRequired
                || execution.lowering != LoweringStatus::NotStarted
                || execution.lowering_error.is_some()
                || execution.image != ExecutionImageStatus::NotReady)
        {
            return Err("non-selected receipt claims an actual resource or image".into());
        }
        if let Some(selection) = execution.selection_identity {
            if selection.artifact != execution.artifact_identity.artifact
                || selection.grant_class != execution.actual_class
                || selection.physical_fingerprint != execution.actual_fingerprint
            {
                return Err("selection identity does not match actual admission".into());
            }
        }
        if execution.admission != AdmissionResult::Selected
            && execution.terminal != ExecutionTerminal::NotExecuted
        {
            return Err("non-selected receipt claims execution".into());
        }
        if execution.image == ExecutionImageStatus::Ready
            && execution.lowering != LoweringStatus::Ready
        {
            return Err("ready image has no successful lowering".into());
        }
        if execution.lowering == LoweringStatus::Failed && execution.lowering_error.is_none() {
            return Err("failed lowering lacks its original error".into());
        }
        if execution.reservation == ResourceReservationStatus::Failed
            && (execution.terminal != ExecutionTerminal::Failed
                || execution.lowering != LoweringStatus::NotStarted
                || execution.image != ExecutionImageStatus::NotReady
                || execution.terminal_error.is_none())
        {
            return Err("failed reservation has an inconsistent lifecycle".into());
        }
        if execution.reservation == ResourceReservationStatus::Failed
            && execution.lowering_error.is_some()
        {
            return Err("failed reservation must not claim lowering".into());
        }
        if execution.lowering == LoweringStatus::Failed
            && (execution.terminal != ExecutionTerminal::Failed
                || execution.image != ExecutionImageStatus::NotReady
                || execution.terminal_error.is_none())
        {
            return Err("failed lowering has an inconsistent lifecycle".into());
        }
        if execution.lowering == LoweringStatus::NotStarted
            && execution.image != ExecutionImageStatus::NotReady
        {
            return Err("image is ready before lowering".into());
        }
        if matches!(
            execution.terminal,
            ExecutionTerminal::Failed | ExecutionTerminal::Cancelled
        ) && execution.terminal_error.is_none()
        {
            return Err("failed execution lacks its original error".into());
        }
        if execution.terminal == ExecutionTerminal::Completed
            && (execution.reservation != ResourceReservationStatus::Committed
                || execution.lowering != LoweringStatus::Ready
                || execution.image != ExecutionImageStatus::Ready)
        {
            return Err("completed execution has incomplete lifecycle phases".into());
        }
    }
    if r.rules.windows(2).any(|pair| pair[0].id >= pair[1].id)
        || r.variants
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err("duplicate or unordered bounded identity".into());
    }
    if let Observation::Observed(count) = r.variant_count {
        if (r.variants.len() as u64).checked_add(r.omitted_variants) != Some(count as u64)
            || r.variants.iter().any(|v| usize::from(v.ordinal) >= count)
        {
            return Err("portfolio coverage does not close".into());
        }
    }
    if let Observation::Observed(fingerprint) = r.selected_fingerprint {
        let Observation::Observed(class) = r.expected_class else {
            return Err("selected artifact has no expected class".into());
        };
        let bit = 1u64
            .checked_shl(class)
            .ok_or("unrepresented expected class")?;
        if !r
            .variants
            .iter()
            .any(|v| v.physical_fingerprint == fingerprint && v.admissible_classes & bit != 0)
        {
            return Err("selected artifact is not represented in its portfolio".into());
        }
    }
    if r.admission != Observation::NotExecuted || r.execution != Observation::NotExecuted {
        return Err("Summary cannot claim target execution".into());
    }
    if r.search_complete == Observation::Observed(true) && r.obligations != Observation::Observed(0)
    {
        return Err("complete search has unresolved or uncovered obligations".into());
    }
    if r.search_stop == Observation::Observed(SearchStop::Complete)
        && (r.search_complete != Observation::Observed(true)
            || r.budget_limited != Observation::Observed(false))
    {
        return Err("Complete requires complete non-budget-limited search".into());
    }
    if r.search_complete == Observation::Observed(true)
        && (r.search_stop != Observation::Observed(SearchStop::Complete)
            || r.budget_limited != Observation::Observed(false))
    {
        return Err("complete search contradicts stop or resource status".into());
    }
    if r.search_stop == Observation::Observed(SearchStop::QualityPolicySatisfied)
        && r.quality_policy_satisfied != Observation::Observed(true)
    {
        return Err("quality stop requires satisfied policy".into());
    }
    if r.search_stop == Observation::Observed(SearchStop::BudgetLimited)
        && r.budget_limited != Observation::Observed(true)
    {
        return Err("budget stop requires budget-limited status".into());
    }
    if r.outcome == CompileOutcome::Success
        && (r.artifact != ArtifactStatus::CompiledArtifactReady
            || r.safety_verified != Observation::Observed(true)
            || !matches!(r.artifact_identity, Observation::Observed(_)))
    {
        return Err("successful compiler record lacks a verified artifact".into());
    }
    if let (
        Observation::Observed(total),
        Observation::Observed(bind),
        Observation::Observed(opt),
        Observation::Observed(verify),
        Observation::Observed(finish),
        Observation::Observed(other),
    ) = (
        r.compiler_ns,
        r.bind_ns,
        r.optimizer_ns,
        r.verify_ns,
        r.finish_ns,
        r.compiler_other_ns,
    ) {
        if [bind, opt, verify, finish, other]
            .into_iter()
            .try_fold(0u64, |a, b| a.checked_add(b))
            != Some(total)
        {
            return Err("compiler phase accounting does not close".into());
        }
    } else if r.outcome == CompileOutcome::Success {
        return Err("successful compiler record lacks phase accounting".into());
    }
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_context::compile_diagnostics::CompileCapture;
    #[test]
    fn schema_and_capacity_rejects_fabricated_execution() {
        let capture = CompileCapture::try_start().unwrap();
        let json = render(&capture.seal(), true);
        validate_json(json.as_bytes()).unwrap();
        assert!(validate_json(json.replace("NotExecuted", "NotApplicable").as_bytes()).is_err());
        assert!(validate_json(
            json.replace("\"schema_version\":3", "\"schema_version\":2")
                .as_bytes()
        )
        .is_err());
        assert!(validate_json(&vec![b' '; ENCODED_LIMIT + 1]).is_err());
        assert!(json.len() < 4096);
        let mut writer = LimitedWriter(Vec::new());
        writer.write_all(&vec![b'x'; ENCODED_LIMIT]).unwrap();
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.0.len(), ENCODED_LIMIT);
        let invalid = CompileCapture::try_start().unwrap();
        invalid.update(|r| {
            r.variant_count = paro_context::compile_diagnostics::Observation::Observed(1)
        });
        assert!(validate_json(render(&invalid.seal(), true).as_bytes()).is_err());
    }

    #[test]
    fn detail_streams_use_owned_sequences_and_typed_parent_ordinals() {
        use paro_context::compile_diagnostics::*;

        let capture = CompileCapture::try_start_with_level(CaptureLevel::Detail).unwrap();
        capture.detail(DetailEvent::Candidate {
            source_sequence: 10,
            event_time_us: 11,
            stage: 2,
            group: MemoGroupRef(1),
            goal: None,
            candidate: Some(CandidateRef(7)),
            source: None,
            source_child: None,
            logical: None,
            physical: None,
            recipe: None,
            rule: None,
            expected_cost_bits: None,
            upper_cost_bits: None,
        });
        capture.detail(DetailEvent::CandidateChild {
            source_sequence: 0,
            parent_event_id: 10,
            ordinal: 0,
            event_time_us: 12,
            stage: 2,
            candidate: Some(CandidateRef(7)),
            child_group: MemoGroupRef(2),
            child_candidate: CandidateRef(8),
            goal: GoalRef {
                required: 3,
                grant: 4,
                context: 5,
            },
        });
        capture.detail(DetailEvent::Fact {
            source_sequence: 0,
            parent_event_id: 10,
            ordinal: 0,
            event_time_us: 13,
            candidate: Some(CandidateRef(7)),
            group: MemoGroupRef(1),
            logical_fact: FingerprintRef([6, 7]),
            statistics_snapshot: FingerprintRef([8, 9]),
        });
        let json = render(&capture.seal(), true);
        validate_json(json.as_bytes()).unwrap();

        let mut invalid: serde_json::Value = serde_json::from_str(&json).unwrap();
        let detail = invalid
            .get_mut("detail")
            .and_then(serde_json::Value::as_array_mut)
            .unwrap();
        detail[1]["data"]["parent_event_id"] = serde_json::json!(999);
        assert!(validate_json(serde_json::to_string(&invalid).unwrap().as_bytes()).is_err());

        let mut invalid: serde_json::Value = serde_json::from_str(&json).unwrap();
        let detail = invalid
            .get_mut("detail")
            .and_then(serde_json::Value::as_array_mut)
            .unwrap();
        detail[1]["data"]["ordinal"] = serde_json::json!(0);
        detail.push(detail[1].clone());
        assert!(validate_json(serde_json::to_string(&invalid).unwrap().as_bytes()).is_err());
    }

    #[test]
    fn observation_wire_fixture_matches_rust_producer() {
        use paro_context::compile_diagnostics::{Observation, UncoveredReason};
        let values: [Observation<u64>; 7] = [
            Observation::NotExecuted,
            Observation::NotApplicable,
            Observation::Observed(0),
            Observation::Observed(u64::MAX),
            Observation::Uncovered(UncoveredReason::NotInstrumented),
            Observation::Uncovered(UncoveredReason::FutureBoundary),
            Observation::Uncovered(UncoveredReason::Capacity),
        ];
        assert_eq!(
            serde_json::to_string(&values).unwrap(),
            r#"["NotExecuted","NotApplicable",{"Observed":0},{"Observed":18446744073709551615},{"Uncovered":"NotInstrumented"},{"Uncovered":"FutureBoundary"},{"Uncovered":"Capacity"}]"#
        );
    }

    #[test]
    fn unavailable_and_terminal_consistency_share_the_reader() {
        use paro_context::compile_diagnostics::*;
        assert!(matches!(
            validate_json(unavailable(true).as_bytes()).unwrap(),
            CompileDocument::Unavailable(_)
        ));
        let process = serde_json::to_vec(&CompileDocument::unavailable(
            UnavailableReason::ProcessCapacity,
        ))
        .unwrap();
        assert!(matches!(
            validate_json(&process).unwrap(),
            CompileDocument::Unavailable(_)
        ));
        let capture = CompileCapture::try_start().unwrap();
        capture.update(|r| {
            r.search_stop = Observation::Observed(SearchStop::Complete);
            r.search_complete = Observation::Observed(false);
            r.obligations = Observation::Observed(5);
            r.budget_limited = Observation::Observed(true);
        });
        assert!(validate_json(render(&capture.seal(), true).as_bytes()).is_err());
        let capture = CompileCapture::try_start().unwrap();
        capture.update(|r| {
            r.outcome = CompileOutcome::Success;
            r.safety_verified = Observation::Observed(true);
            r.artifact = ArtifactStatus::CompiledArtifactReady;
        });
        assert!(validate_json(render(&capture.seal(), true).as_bytes()).is_err());
    }

    #[test]
    fn execution_receipt_validates_selection_reservation_and_lowering_order() {
        use paro_context::compile_diagnostics::*;

        let identity = ArtifactIdentity {
            schema_version: IDENTITY_SCHEMA_VERSION,
            artifact: CompiledArtifactId([1, 2]),
            structure: PlanStructureId([3, 4]),
            dependencies: [5, 6],
        };
        let resources = ResourceReceipt {
            class: 2,
            minimum_memory_bytes: 100,
            working_set_memory_bytes: 200,
            memory_ceiling_bytes: 1_000,
            memory_completion: MemoryCompletionReceipt::Guaranteed,
            max_parallel_tasks: 4,
            external_worker_slots: 0,
        };
        let receipt = ExecutionReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            execution_id: ExecutionReceiptId(7),
            admission_receipt_id: AdmissionReceiptId(7),
            selection_identity: Some(SelectionIdentity {
                artifact: identity.artifact,
                grant_class: Some(2),
                physical_fingerprint: Some([7, 8]),
            }),
            statement_decision_id: Some(4),
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([7, 8]),
            resources: Some(resources),
            admission: AdmissionResult::Selected,
            fallback: None,
            reservation: ResourceReservationStatus::Committed,
            lowering: LoweringStatus::Ready,
            lowering_error: None,
            image: ExecutionImageStatus::Ready,
            terminal: ExecutionTerminal::Completed,
            terminal_error: None,
        };

        let capture = CompileCapture::try_start().unwrap();
        capture.update(|record| {
            record.outcome = CompileOutcome::Success;
            record.safety_verified = Observation::Observed(true);
            record.artifact = ArtifactStatus::CompiledArtifactReady;
            record.artifact_identity = Observation::Observed(identity);
            record.compiler_ns = Observation::Observed(0);
            record.bind_ns = Observation::Observed(0);
            record.optimizer_ns = Observation::Observed(0);
            record.verify_ns = Observation::Observed(0);
            record.finish_ns = Observation::Observed(0);
            record.compiler_other_ns = Observation::Observed(0);
        });
        let json = render_with_execution(&capture.seal(), true, Some(receipt.clone()));
        validate_json(json.as_bytes()).unwrap();

        let mut invalid = serde_json::from_str::<serde_json::Value>(&json).unwrap();
        let execution = invalid
            .get_mut("execution_receipt")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        execution.insert("image".into(), serde_json::Value::String("NotReady".into()));
        assert!(validate_json(serde_json::to_string(&invalid).unwrap().as_bytes()).is_err());

        let capture = CompileCapture::try_start().unwrap();
        capture.update(|record| {
            record.outcome = CompileOutcome::Success;
            record.safety_verified = Observation::Observed(true);
            record.artifact = ArtifactStatus::CompiledArtifactReady;
            record.artifact_identity = Observation::Observed(identity);
            record.compiler_ns = Observation::Observed(0);
            record.bind_ns = Observation::Observed(0);
            record.optimizer_ns = Observation::Observed(0);
            record.verify_ns = Observation::Observed(0);
            record.finish_ns = Observation::Observed(0);
            record.compiler_other_ns = Observation::Observed(0);
        });
        let mut invalid = serde_json::from_str::<serde_json::Value>(&render_with_execution(
            &capture.seal(),
            true,
            Some(receipt),
        ))
        .unwrap();
        let execution = invalid
            .get_mut("execution_receipt")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        execution.insert(
            "reservation".into(),
            serde_json::Value::String("Failed".into()),
        );
        execution.insert(
            "terminal".into(),
            serde_json::Value::String("Completed".into()),
        );
        assert!(validate_json(serde_json::to_string(&invalid).unwrap().as_bytes()).is_err());
    }

    #[test]
    fn detail_overflow_preserves_analyze_receipt_and_summary() {
        use paro_context::compile_diagnostics::*;

        let identity = ArtifactIdentity {
            schema_version: IDENTITY_SCHEMA_VERSION,
            artifact: CompiledArtifactId([101, 102]),
            structure: PlanStructureId([103, 104]),
            dependencies: [105, 106],
        };
        let receipt = ExecutionReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            execution_id: ExecutionReceiptId(99),
            admission_receipt_id: AdmissionReceiptId(99),
            selection_identity: Some(SelectionIdentity {
                artifact: identity.artifact,
                grant_class: Some(2),
                physical_fingerprint: Some([107, 108]),
            }),
            statement_decision_id: Some(7),
            artifact_identity: identity,
            expected_class: Some(2),
            actual_class: Some(2),
            actual_fingerprint: Some([107, 108]),
            resources: Some(ResourceReceipt {
                class: 2,
                minimum_memory_bytes: 100,
                working_set_memory_bytes: 200,
                memory_ceiling_bytes: 300,
                memory_completion: MemoryCompletionReceipt::Guaranteed,
                max_parallel_tasks: 4,
                external_worker_slots: 0,
            }),
            admission: AdmissionResult::Selected,
            fallback: None,
            reservation: ResourceReservationStatus::Committed,
            lowering: LoweringStatus::Ready,
            lowering_error: None,
            image: ExecutionImageStatus::Ready,
            terminal: ExecutionTerminal::Completed,
            terminal_error: None,
        };
        let capture = CompileCapture::try_start_with_level(CaptureLevel::Detail).unwrap();
        capture.update(|record| {
            record.outcome = CompileOutcome::Success;
            record.safety_verified = Observation::Observed(true);
            record.artifact = ArtifactStatus::CompiledArtifactReady;
            record.artifact_identity = Observation::Observed(identity);
            record.compiler_ns = Observation::Observed(0);
            record.bind_ns = Observation::Observed(0);
            record.optimizer_ns = Observation::Observed(0);
            record.verify_ns = Observation::Observed(0);
            record.finish_ns = Observation::Observed(0);
            record.compiler_other_ns = Observation::Observed(0);
        });
        for sequence in 0..MAX_DETAIL_EVENTS {
            capture.detail(DetailEvent::Task {
                source_sequence: sequence as u64,
                event_time_us: sequence as u64,
                group: MemoGroupRef(sequence as u64),
                expression: LogicalExprRef(sequence as u64),
                rule: RuleRef(1),
                first_binding: None,
                first_run_us: None,
                first_published_us: None,
                match_count: 1,
                applicable_count: 1,
                published_count: 1,
                no_match_count: 0,
                no_output_count: 0,
                budget_rejected_count: 0,
            });
        }
        let json = render_with_execution(&capture.seal(), true, Some(receipt));
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("detail").is_some());
        assert!(value.get("execution_receipt").is_some());
        assert!(
            value
                .get("omitted_encoding_detail")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
                > 0
        );
        assert!(!json.contains("DiagnosticUnavailable"));
        validate_json(json.as_bytes()).unwrap();
    }
}
