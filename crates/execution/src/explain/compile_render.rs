// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Render only the sealed target observation; never invoke planning or admission.
use paro_context::compile_diagnostics::{CompileCapture, ENCODED_LIMIT};
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

pub fn render(capture: &CompileCapture, json: bool) -> String {
    let mut writer = LimitedWriter(Vec::new());
    if writer.0.try_reserve_exact(ENCODED_LIMIT).is_err() {
        return unavailable(json);
    }
    let result = capture.read(|record| {
        if json { serde_json::to_writer(&mut writer, record).map_err(io::Error::other) }
        else {
            writeln!(writer, "EXPLAIN (COMPILE) / schema {} / ForcedCompile", record.schema_version)?;
            writeln!(writer, "phase             nanoseconds (Observed / Uncovered)")?;
            writeln!(writer, "bind              {:?}\noptimizer         {:?}\nverify            {:?}\nfinish            {:?}\ncompiler wall     {:?}", record.bind_ns, record.optimizer_ns, record.verify_ns, record.finish_ns, record.compiler_ns)?;
            writeln!(writer, "compiler other    {:?}\nparse             {:?}", record.compiler_other_ns, record.parse)?;
            writeln!(writer, "artifact={:?} safety={:?} stop={:?} complete={:?} obligations={:?}", record.artifact, record.safety_verified, record.search_stop, record.search_complete, record.obligations)?;
            writeln!(writer, "quality_satisfied={:?} budget_limited={:?} groups={:?} logical={:?} physical={:?}", record.quality_policy_satisfied, record.budget_limited, record.groups, record.logical_expressions, record.physical_expressions)?;
            writeln!(writer, "input={:?} output={:?} settings={:?} memory_bytes={:?} parallel_tasks={:?}", record.input_fingerprint, record.output_identity, record.planning_settings, record.available_memory_bytes, record.available_parallel_tasks)?;
            writeln!(writer, "expected_class={:?} variants={:?} selected_fingerprint={:?}", record.expected_class, record.variant_count, record.selected_fingerprint)?;
            for v in &record.variants { writeln!(writer, "variant {} fingerprint={:?} admissible_classes={}", v.ordinal,v.physical_fingerprint,v.admissible_classes)?; }
            writeln!(writer, "admission={:?} execution={:?}", record.admission, record.execution)?;
            for r in &record.rules { writeln!(writer, "rule {} attempts={} inserted={} elapsed_ns={}",r.id,r.attempts,r.inserted,r.elapsed_ns)?; }
            writeln!(writer, "omitted_rules={} omitted_variants={} retained_limit={} encoded_limit={} response_terminal={:?}",record.omitted_rules,record.omitted_variants,record.retained_limit,record.encoded_limit,record.response_terminal)
        }
    });
    if result.is_err() {
        return unavailable(json);
    }
    String::from_utf8(writer.0).expect("UTF-8 renderer")
}

pub fn unavailable(json: bool) -> String {
    if json {
        r#"{"schema_version":1,"diagnostic":"Unavailable","reason":"Capacity"}"#.into()
    } else {
        "EXPLAIN (COMPILE): DiagnosticUnavailable(Capacity)".into()
    }
}

/// Current schema only. Size is checked before deserializing untrusted input.
pub fn validate_json(
    bytes: &[u8],
) -> Result<paro_context::compile_diagnostics::CompileRecord, String> {
    use paro_context::compile_diagnostics::*;
    if bytes.len() > ENCODED_LIMIT {
        return Err("encoded capacity exceeded".into());
    }
    let r: CompileRecord = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if r.schema_version != SCHEMA_VERSION
        || r.encoded_limit != ENCODED_LIMIT
        || r.retained_limit != RETAINED_LIMIT
        || r.process_limit != PROCESS_LIMIT
        || r.rules.len() > MAX_RULES
        || r.variants.len() > MAX_VARIANTS
    {
        return Err("schema/capacity profile mismatch".into());
    }
    if r.admission != Observation::NotExecuted || r.execution != Observation::NotExecuted {
        return Err("Summary cannot claim target execution".into());
    }
    if r.search_complete == Observation::Observed(true) && r.obligations != Observation::Observed(0)
    {
        return Err("complete search has unresolved or uncovered obligations".into());
    }
    if r.outcome == CompileOutcome::Success
        && (r.artifact != ArtifactStatus::CompiledArtifactReady
            || r.safety_verified != Observation::Observed(true))
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
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_and_capacity_rejects_fabricated_execution() {
        let capture = CompileCapture::try_start().unwrap();
        let json = render(&capture, true);
        validate_json(json.as_bytes()).unwrap();
        assert!(validate_json(json.replace("NotExecuted", "NotApplicable").as_bytes()).is_err());
        assert!(validate_json(
            json.replace("\"schema_version\":1", "\"schema_version\":2")
                .as_bytes()
        )
        .is_err());
        assert!(validate_json(&vec![b' '; ENCODED_LIMIT + 1]).is_err());
        assert!(json.len() < 4096);
        let mut writer = LimitedWriter(Vec::new());
        writer.write_all(&vec![b'x'; ENCODED_LIMIT]).unwrap();
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.0.len(), ENCODED_LIMIT);
    }
}
