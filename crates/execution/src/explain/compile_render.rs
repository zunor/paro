// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Render only the sealed target observation; never invoke planning or admission.
use paro_context::compile_diagnostics::{
    CompileRecord, ENCODED_LIMIT, ExecutionReceipt, SealedCompileCapture,
};
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
    let mut writer = LimitedWriter(Vec::new());
    if writer.0.try_reserve_exact(ENCODED_LIMIT).is_err() {
        return unavailable(json);
    }
    let result = capture.read(|record| {
        let mut record: CompileRecord = record.clone();
        record.execution_receipt = execution;
        if json { serde_json::to_writer(&mut writer, &record).map_err(io::Error::other) }
        else {
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
            writeln!(writer, "omitted_rules={} omitted_variants={} retained_limit={} encoded_limit={} response_terminal={:?} execution_receipt={:?}",record.omitted_rules,record.omitted_variants,record.retained_limit,record.encoded_limit,record.response_terminal,record.execution_receipt)
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
        || r.variants.len() > MAX_VARIANTS
    {
        return Err("schema/capacity profile mismatch".into());
    }
    if let Some(execution) = &r.execution_receipt {
        if execution.schema_version != paro_context::compile_diagnostics::RECEIPT_SCHEMA_VERSION
            || r.artifact_identity != Observation::Observed(execution.artifact_identity)
        {
            return Err("execution receipt is not bound to the sealed compile artifact".into());
        }
        if execution.admission == AdmissionResult::Selected
            && (execution.actual_class.is_none()
                || execution.actual_fingerprint.is_none()
                || execution.resources.is_none())
        {
            return Err("selected execution receipt lacks actual admission".into());
        }
        if execution.admission != AdmissionResult::Selected
            && execution.terminal != ExecutionTerminal::NotExecuted
        {
            return Err("non-selected admission cannot have an execution terminal".into());
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
        assert!(
            validate_json(
                json.replace("\"schema_version\":2", "\"schema_version\":1")
                    .as_bytes()
            )
            .is_err()
        );
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
}
