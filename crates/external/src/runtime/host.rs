// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::process::Command;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use paro_common::error::{self as paro_error, Result};
use tracing::{info, warn};

use crate::runtime::artifact::gc::ArtifactGcPolicy;
use crate::runtime::dispatch::permits::PermitBudget;
use crate::runtime::dispatch::policy::ExternalDispatchPolicy;
use crate::runtime::metrics::autotuning::AutotuningPolicy;
use crate::runtime::protocol::version::RuntimeProtocolVersion;
use crate::runtime::worker::pool::WorkerPoolPolicy;

const DEFAULT_REPROBE_BASE: Duration = Duration::from_secs(1);
const DEFAULT_REPROBE_MAX: Duration = Duration::from_secs(30);
const DISABLE_ENV: &str = "PARO_PYTHON_RUNTIME_DISABLED";
const PYTHON_BIN_ENV: &str = "PARO_PYTHON_BIN";
const DEFAULT_PYTHON_BIN: &str = "python3";
const PYTHON_SLOTS_PROBE: &str = "import dataclasses\n@dataclasses.dataclass(slots=True)\nclass _ParoSlotsProbe:\n    value: int\n";
const PYTHON_AUTO_CANDIDATES: &[&str] = &[
    "python3.12",
    "python3.11",
    "python3.13",
    "python3.14",
    DEFAULT_PYTHON_BIN,
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PythonRuntimeStartupPolicy {
    #[default]
    LazyBestEffort,
    BackgroundProbe,
    StrictFailFast,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PythonRuntimeAvailability {
    #[default]
    NotProbed,
    Ready,
    Degraded {
        last_error: String,
    },
    DisabledByConfig,
    BinaryMissing,
    Misconfigured,
}

impl PythonRuntimeAvailability {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::NotProbed => "not_probed",
            Self::Ready => "ready",
            Self::Degraded { .. } => "degraded",
            Self::DisabledByConfig => "disabled_by_config",
            Self::BinaryMissing => "binary_missing",
            Self::Misconfigured => "misconfigured",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonRuntimeStatus {
    pub startup_policy: PythonRuntimeStartupPolicy,
    pub availability: PythonRuntimeAvailability,
    pub detail: Option<String>,
    pub consecutive_failures: u32,
    pub next_probe_in: Option<Duration>,
}

impl PythonRuntimeStatus {
    pub fn is_ready(&self) -> bool {
        self.availability.is_ready()
    }

    pub fn availability_label(&self) -> &'static str {
        self.availability.label()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonRuntimeProbeResult {
    pub availability: PythonRuntimeAvailability,
    pub detail: Option<String>,
}

impl PythonRuntimeProbeResult {
    pub fn ready() -> Self {
        Self {
            availability: PythonRuntimeAvailability::Ready,
            detail: None,
        }
    }

    pub fn disabled_by_config(detail: impl Into<String>) -> Self {
        Self {
            availability: PythonRuntimeAvailability::DisabledByConfig,
            detail: Some(detail.into()),
        }
    }

    pub fn binary_missing(detail: impl Into<String>) -> Self {
        Self {
            availability: PythonRuntimeAvailability::BinaryMissing,
            detail: Some(detail.into()),
        }
    }

    pub fn misconfigured(detail: impl Into<String>) -> Self {
        Self {
            availability: PythonRuntimeAvailability::Misconfigured,
            detail: Some(detail.into()),
        }
    }
}

pub trait PythonRuntimeProbe: Send + Sync {
    fn probe(&self) -> PythonRuntimeProbeResult;
}

pub trait PythonRuntimeProvider: Send + Sync {
    fn startup_policy(&self) -> PythonRuntimeStartupPolicy;
    fn status(&self) -> PythonRuntimeStatus;
    fn ensure_ready_for_ddl(&self) -> Result<()>;
    fn ensure_ready_for_execution(&self) -> Result<()>;
    fn force_reprobe(&self) -> PythonRuntimeStatus;
    fn observe_worker_failure(&self, _message: &str) -> PythonRuntimeStatus {
        self.status()
    }
}

#[derive(Debug)]
struct CommandPythonRuntimeProbe {
    python_binary: OsString,
}

impl Default for CommandPythonRuntimeProbe {
    fn default() -> Self {
        let python_binary = default_python_binary();
        Self { python_binary }
    }
}

impl PythonRuntimeProbe for CommandPythonRuntimeProbe {
    fn probe(&self) -> PythonRuntimeProbeResult {
        if env_flag_enabled(DISABLE_ENV) {
            return PythonRuntimeProbeResult::disabled_by_config(
                "Python runtime is disabled by configuration",
            );
        }

        match Command::new(&self.python_binary)
            .arg("-c")
            .arg(PYTHON_SLOTS_PROBE)
            .output()
        {
            Ok(output) if output.status.success() => PythonRuntimeProbeResult::ready(),
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let detail = if stderr.is_empty() {
                    format!("python bootstrap exited with status {}", output.status)
                } else {
                    stderr
                };
                PythonRuntimeProbeResult::misconfigured(format!(
                    "failed to bootstrap Python interpreter '{}': {detail}",
                    display_os(&self.python_binary)
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                PythonRuntimeProbeResult::binary_missing(format!(
                    "Python interpreter '{}' was not found",
                    display_os(&self.python_binary)
                ))
            }
            Err(error) => PythonRuntimeProbeResult::misconfigured(format!(
                "failed to spawn Python interpreter '{}': {error}",
                display_os(&self.python_binary)
            )),
        }
    }
}

pub fn default_python_binary() -> OsString {
    if let Some(value) = env::var_os(PYTHON_BIN_ENV).filter(|value| !value.is_empty()) {
        return value;
    }
    PYTHON_AUTO_CANDIDATES
        .iter()
        .copied()
        .find(|candidate| python_supports_required_dataclass_slots(candidate))
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from(DEFAULT_PYTHON_BIN))
}

fn python_supports_required_dataclass_slots(candidate: &str) -> bool {
    Command::new(candidate)
        .arg("-c")
        .arg(PYTHON_SLOTS_PROBE)
        .output()
        .is_ok_and(|output| output.status.success())
}

#[derive(Debug, Clone)]
struct PythonRuntimeState {
    availability: PythonRuntimeAvailability,
    detail: Option<String>,
    consecutive_failures: u32,
    next_probe_at: Option<Instant>,
}

impl Default for PythonRuntimeState {
    fn default() -> Self {
        Self {
            availability: PythonRuntimeAvailability::NotProbed,
            detail: None,
            consecutive_failures: 0,
            next_probe_at: None,
        }
    }
}

pub struct ExternalRuntimeHost {
    protocol: RuntimeProtocolVersion,
    startup_policy: PythonRuntimeStartupPolicy,
    dispatch_policy: ExternalDispatchPolicy,
    worker_pool_policy: WorkerPoolPolicy,
    permit_budget: PermitBudget,
    artifact_gc_policy: ArtifactGcPolicy,
    autotuning_policy: AutotuningPolicy,
    degraded_retry_base: Duration,
    degraded_retry_max: Duration,
    probe: Arc<dyn PythonRuntimeProbe>,
    state: RwLock<PythonRuntimeState>,
}

impl Default for ExternalRuntimeHost {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ExternalRuntimeHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalRuntimeHost")
            .field("protocol", &self.protocol)
            .field("startup_policy", &self.startup_policy)
            .field("dispatch_policy", &self.dispatch_policy)
            .field("worker_pool_policy", &self.worker_pool_policy)
            .field("permit_budget", &self.permit_budget)
            .field("artifact_gc_policy", &self.artifact_gc_policy)
            .field("autotuning_policy", &self.autotuning_policy)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ExternalRuntimeHost {
    pub fn new() -> Self {
        Self {
            protocol: RuntimeProtocolVersion::current(),
            startup_policy: PythonRuntimeStartupPolicy::LazyBestEffort,
            dispatch_policy: ExternalDispatchPolicy::default(),
            worker_pool_policy: WorkerPoolPolicy::default(),
            permit_budget: PermitBudget::default(),
            artifact_gc_policy: ArtifactGcPolicy::default(),
            autotuning_policy: AutotuningPolicy::default(),
            degraded_retry_base: DEFAULT_REPROBE_BASE,
            degraded_retry_max: DEFAULT_REPROBE_MAX,
            probe: Arc::new(CommandPythonRuntimeProbe::default()),
            state: RwLock::new(PythonRuntimeState::default()),
        }
    }

    pub fn ready_stub() -> Self {
        let host = Self::new();
        host.set_state(
            PythonRuntimeAvailability::Ready,
            None,
            0,
            None,
            Some("initialized ready stub"),
        );
        host
    }

    pub fn with_startup_policy(mut self, startup_policy: PythonRuntimeStartupPolicy) -> Self {
        self.startup_policy = startup_policy;
        self
    }

    pub fn with_probe(mut self, probe: Arc<dyn PythonRuntimeProbe>) -> Self {
        self.probe = probe;
        self
    }

    pub fn with_reprobe_window(mut self, base: Duration, max: Duration) -> Self {
        self.degraded_retry_base = base;
        self.degraded_retry_max = max.max(base);
        self
    }

    pub fn protocol(&self) -> RuntimeProtocolVersion {
        self.protocol
    }

    pub fn dispatch_policy(&self) -> &ExternalDispatchPolicy {
        &self.dispatch_policy
    }

    pub fn worker_pool_policy(&self) -> &WorkerPoolPolicy {
        &self.worker_pool_policy
    }

    pub fn permit_budget(&self) -> &PermitBudget {
        &self.permit_budget
    }

    pub fn artifact_gc_policy(&self) -> &ArtifactGcPolicy {
        &self.artifact_gc_policy
    }

    pub fn autotuning_policy(&self) -> &AutotuningPolicy {
        &self.autotuning_policy
    }

    pub fn startup_policy_value(&self) -> PythonRuntimeStartupPolicy {
        self.startup_policy
    }

    pub fn availability(&self) -> PythonRuntimeAvailability {
        self.status().availability
    }

    pub fn status(&self) -> PythonRuntimeStatus {
        let state = self.state.read().expect("python runtime state");
        PythonRuntimeStatus {
            startup_policy: self.startup_policy,
            availability: state.availability.clone(),
            detail: state.detail.clone(),
            consecutive_failures: state.consecutive_failures,
            next_probe_in: state
                .next_probe_at
                .map(|instant| instant.saturating_duration_since(Instant::now())),
        }
    }

    pub fn maybe_reprobe(&self) -> Option<PythonRuntimeStatus> {
        let should_reprobe = {
            let state = self.state.read().expect("python runtime state");
            matches!(
                state.availability,
                PythonRuntimeAvailability::Degraded { .. }
            ) && state
                .next_probe_at
                .is_some_and(|next_probe_at| Instant::now() >= next_probe_at)
        };
        should_reprobe.then(|| self.force_reprobe())
    }

    pub fn observe_worker_failure(&self, message: impl Into<String>) -> PythonRuntimeStatus {
        let last_error = message.into();
        let next_state = {
            let mut state = self.state.write().expect("python runtime state");
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            let next_probe_at = Instant::now()
                + backoff_delay(
                    state.consecutive_failures,
                    self.degraded_retry_base,
                    self.degraded_retry_max,
                );
            state.availability = PythonRuntimeAvailability::Degraded {
                last_error: last_error.clone(),
            };
            state.detail = Some(last_error.clone());
            state.next_probe_at = Some(next_probe_at);
            state.clone()
        };

        warn!(
            availability = next_state.availability.label(),
            consecutive_failures = next_state.consecutive_failures,
            "Python runtime degraded: {last_error}"
        );
        self.status()
    }

    fn ensure_ready(&self, operation: &str) -> Result<()> {
        let should_probe = {
            let state = self.state.read().expect("python runtime state");
            match &state.availability {
                PythonRuntimeAvailability::Ready => return Ok(()),
                PythonRuntimeAvailability::NotProbed => true,
                PythonRuntimeAvailability::Degraded { .. } => state
                    .next_probe_at
                    .is_some_and(|next_probe_at| Instant::now() >= next_probe_at),
                PythonRuntimeAvailability::DisabledByConfig
                | PythonRuntimeAvailability::BinaryMissing
                | PythonRuntimeAvailability::Misconfigured => false,
            }
        };

        if should_probe {
            self.run_probe();
        }

        let status = self.status();
        if status.is_ready() {
            return Ok(());
        }

        Err(runtime_unavailable_error(&status, operation))
    }

    fn run_probe(&self) -> PythonRuntimeStatus {
        let result = self.probe.probe();
        let availability = result.availability.clone();
        let detail = result.detail.clone();
        let transition_reason = match &availability {
            PythonRuntimeAvailability::Ready => "runtime probe succeeded".to_string(),
            PythonRuntimeAvailability::DisabledByConfig => {
                "runtime probe found configuration disable".to_string()
            }
            PythonRuntimeAvailability::BinaryMissing => {
                "runtime probe could not find python binary".to_string()
            }
            PythonRuntimeAvailability::Misconfigured => {
                "runtime probe found runtime misconfiguration".to_string()
            }
            PythonRuntimeAvailability::NotProbed => {
                "runtime probe returned not-probed state".to_string()
            }
            PythonRuntimeAvailability::Degraded { last_error } => {
                format!("runtime probe remained degraded: {last_error}")
            }
        };
        self.set_state(availability, detail, 0, None, Some(&transition_reason));
        self.status()
    }

    fn set_state(
        &self,
        availability: PythonRuntimeAvailability,
        detail: Option<String>,
        consecutive_failures: u32,
        next_probe_at: Option<Instant>,
        log_reason: Option<&str>,
    ) {
        {
            let mut state = self.state.write().expect("python runtime state");
            state.availability = availability.clone();
            state.detail = detail;
            state.consecutive_failures = consecutive_failures;
            state.next_probe_at = next_probe_at;
        }

        if let Some(reason) = log_reason {
            info!(
                availability = availability.label(),
                startup_policy = ?self.startup_policy,
                "{reason}"
            );
        }
    }
}

impl PythonRuntimeProvider for ExternalRuntimeHost {
    fn startup_policy(&self) -> PythonRuntimeStartupPolicy {
        self.startup_policy
    }

    fn status(&self) -> PythonRuntimeStatus {
        ExternalRuntimeHost::status(self)
    }

    fn ensure_ready_for_ddl(&self) -> Result<()> {
        self.ensure_ready("CREATE FUNCTION")
    }

    fn ensure_ready_for_execution(&self) -> Result<()> {
        self.ensure_ready("external routine execution")
    }

    fn force_reprobe(&self) -> PythonRuntimeStatus {
        self.run_probe()
    }

    fn observe_worker_failure(&self, message: &str) -> PythonRuntimeStatus {
        ExternalRuntimeHost::observe_worker_failure(self, message.to_string())
    }
}

fn runtime_unavailable_error(
    status: &PythonRuntimeStatus,
    operation: &str,
) -> paro_common::error::ParoError {
    let message = match &status.availability {
        PythonRuntimeAvailability::NotProbed => {
            format!("Python runtime has not been probed yet for {operation}")
        }
        PythonRuntimeAvailability::Ready => {
            format!("Python runtime is unexpectedly unavailable for {operation}")
        }
        PythonRuntimeAvailability::Degraded { last_error } => {
            format!("Python runtime is degraded and unavailable for {operation}: {last_error}")
        }
        PythonRuntimeAvailability::DisabledByConfig => {
            format!("Python runtime is disabled by configuration for {operation}")
        }
        PythonRuntimeAvailability::BinaryMissing => {
            format!("Python runtime interpreter is missing for {operation}")
        }
        PythonRuntimeAvailability::Misconfigured => {
            format!("Python runtime is misconfigured for {operation}")
        }
    };

    let mut error = paro_error::python_runtime_unavailable(message);
    if let Some(detail) = &status.detail {
        error = error.detail(detail.clone());
    }
    if let Some(next_probe_in) = status.next_probe_in {
        error = error.hint(format!(
            "next automatic Python runtime probe in {} ms",
            next_probe_in.as_millis()
        ));
    }
    error
}

fn backoff_delay(failures: u32, base: Duration, max: Duration) -> Duration {
    let exponent = failures.saturating_sub(1).min(10);
    let factor = 1_u32 << exponent;
    let scaled = base.checked_mul(factor).unwrap_or(max);
    scaled.min(max)
}

fn env_flag_enabled(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn display_os(value: &OsStr) -> String {
    value.to_string_lossy().into_owned()
}
