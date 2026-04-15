use crate::{DatabaseRecordState, RecoveryHookIssueKind, RecoveryHookResult, RecoveryReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InstanceStartupDisposition {
    #[default]
    FullRecovery,
    CleanFastPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartupPolicy {
    #[default]
    Strict,
    Repair,
    BestEffort,
}

impl StartupPolicy {
    pub fn allows_degraded_startup(self) -> bool {
        matches!(self, Self::Repair | Self::BestEffort)
    }

    pub fn enables_repair_actions(self) -> bool {
        matches!(self, Self::Repair | Self::BestEffort)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseStartupStatus {
    Recovered,
    Reconciled,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupIssueKind {
    OrphanDirectory,
    StorageIdentityMismatch,
    ManifestMismatch,
    RecoveryHookFailure,
    CleanStateInvariantViolation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupIssue {
    pub kind: StartupIssueKind,
    pub database_id: Option<u64>,
    pub name: Option<String>,
    pub path: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseStartupEntry {
    pub database_id: u64,
    pub name: String,
    pub durable_state: DatabaseRecordState,
    pub status: DatabaseStartupStatus,
    pub detail: Option<String>,
    pub recovery_report: Option<RecoveryReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StartupReport {
    pub policy: StartupPolicy,
    pub disposition: InstanceStartupDisposition,
    pub databases: Vec<DatabaseStartupEntry>,
    pub issues: Vec<StartupIssue>,
}

impl StartupReport {
    pub fn new(policy: StartupPolicy, disposition: InstanceStartupDisposition) -> Self {
        Self {
            policy,
            disposition,
            databases: Vec::new(),
            issues: Vec::new(),
        }
    }

    pub fn push(&mut self, entry: DatabaseStartupEntry) {
        self.databases.push(entry);
        self.databases
            .sort_by_key(|database| (database.database_id, database.name.clone()));
    }

    pub fn has_database(&self, database_id: u64) -> bool {
        self.databases
            .iter()
            .any(|database| database.database_id == database_id)
    }

    pub fn record_recovered(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        durable_state: DatabaseRecordState,
        path: impl Into<String>,
        recovery_report: RecoveryReport,
    ) {
        let name = name.into();
        let path = path.into();
        self.push(DatabaseStartupEntry {
            database_id,
            name: name.clone(),
            durable_state,
            status: DatabaseStartupStatus::Recovered,
            detail: None,
            recovery_report: Some(recovery_report.clone()),
        });
        self.record_hook_issues(database_id, &name, &path, &recovery_report);
    }

    pub fn record_reconciled(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        durable_state: DatabaseRecordState,
        detail: impl Into<String>,
    ) {
        self.push(DatabaseStartupEntry {
            database_id,
            name: name.into(),
            durable_state,
            status: DatabaseStartupStatus::Reconciled,
            detail: Some(detail.into()),
            recovery_report: None,
        });
    }

    pub fn record_skipped(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        durable_state: DatabaseRecordState,
        detail: impl Into<String>,
    ) {
        self.push(DatabaseStartupEntry {
            database_id,
            name: name.into(),
            durable_state,
            status: DatabaseStartupStatus::Skipped,
            detail: Some(detail.into()),
            recovery_report: None,
        });
    }

    pub fn record_failed(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        durable_state: DatabaseRecordState,
        detail: impl Into<String>,
    ) {
        self.record_failed_with_report(database_id, name, durable_state, None, detail, None);
    }

    pub fn record_failed_with_report(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        durable_state: DatabaseRecordState,
        path: Option<String>,
        detail: impl Into<String>,
        recovery_report: Option<RecoveryReport>,
    ) {
        let name = name.into();
        let detail = detail.into();
        self.push(DatabaseStartupEntry {
            database_id,
            name: name.clone(),
            durable_state,
            status: DatabaseStartupStatus::Failed,
            detail: Some(detail),
            recovery_report: recovery_report.clone(),
        });
        if let Some(recovery_report) = recovery_report {
            self.record_hook_issues(
                database_id,
                &name,
                path.as_deref().unwrap_or_default(),
                &recovery_report,
            );
        }
    }

    pub fn record_issue(
        &mut self,
        kind: StartupIssueKind,
        database_id: Option<u64>,
        name: Option<String>,
        path: Option<String>,
        detail: impl Into<String>,
    ) {
        self.issues.push(StartupIssue {
            kind,
            database_id,
            name,
            path,
            detail: detail.into(),
        });
        self.issues.sort_by_key(|issue| {
            (
                issue.database_id.unwrap_or(u64::MAX),
                issue.name.clone().unwrap_or_default(),
                issue.path.clone().unwrap_or_default(),
            )
        });
    }

    pub fn record_orphan_directory(&mut self, path: impl Into<String>, detail: impl Into<String>) {
        self.record_issue(
            StartupIssueKind::OrphanDirectory,
            None,
            None,
            Some(path.into()),
            detail,
        );
    }

    pub fn record_identity_mismatch(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        path: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.record_issue(
            StartupIssueKind::StorageIdentityMismatch,
            Some(database_id),
            Some(name.into()),
            Some(path.into()),
            detail,
        );
    }

    pub fn record_manifest_mismatch(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        path: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.record_issue(
            StartupIssueKind::ManifestMismatch,
            Some(database_id),
            Some(name.into()),
            Some(path.into()),
            detail,
        );
    }

    pub fn record_recovery_hook_failure(
        &mut self,
        database_id: u64,
        name: impl Into<String>,
        path: Option<String>,
        detail: impl Into<String>,
    ) {
        self.record_issue(
            StartupIssueKind::RecoveryHookFailure,
            Some(database_id),
            Some(name.into()),
            path,
            detail,
        );
    }

    pub fn record_clean_state_invariant_violation(&mut self, detail: impl Into<String>) {
        self.record_issue(
            StartupIssueKind::CleanStateInvariantViolation,
            None,
            None,
            None,
            detail,
        );
    }

    pub fn counts(&self) -> StartupReportCounts {
        let mut counts = StartupReportCounts::default();
        for entry in &self.databases {
            counts.total += 1;
            match entry.status {
                DatabaseStartupStatus::Recovered => counts.recovered += 1,
                DatabaseStartupStatus::Reconciled => counts.reconciled += 1,
                DatabaseStartupStatus::Skipped => counts.skipped += 1,
                DatabaseStartupStatus::Failed => counts.failed += 1,
            }
        }
        counts.issues = self.issues.len();
        for issue in &self.issues {
            match issue.kind {
                StartupIssueKind::OrphanDirectory => counts.orphan_directories += 1,
                StartupIssueKind::StorageIdentityMismatch => counts.identity_mismatches += 1,
                StartupIssueKind::ManifestMismatch => counts.manifest_mismatches += 1,
                StartupIssueKind::RecoveryHookFailure => counts.recovery_hook_failures += 1,
                StartupIssueKind::CleanStateInvariantViolation => {
                    counts.clean_state_invariant_violations += 1
                }
            }
        }
        counts
    }

    pub fn log_summary(&self) {
        let counts = self.counts();
        tracing::info!(
            policy = ?self.policy,
            disposition = ?self.disposition,
            total = counts.total,
            recovered = counts.recovered,
            reconciled = counts.reconciled,
            skipped = counts.skipped,
            failed = counts.failed,
            issues = counts.issues,
            orphan_directories = counts.orphan_directories,
            identity_mismatches = counts.identity_mismatches,
            manifest_mismatches = counts.manifest_mismatches,
            recovery_hook_failures = counts.recovery_hook_failures,
            clean_state_invariant_violations = counts.clean_state_invariant_violations,
            "Instance startup summary"
        );
    }

    fn record_hook_issues(
        &mut self,
        database_id: u64,
        name: &str,
        path: &str,
        recovery_report: &RecoveryReport,
    ) {
        for hook_result in &recovery_report.hook_results {
            match hook_result {
                RecoveryHookResult::Rebuilt { issues, .. }
                | RecoveryHookResult::Failed { issues, .. } => {
                    for issue in issues {
                        match issue.kind {
                            RecoveryHookIssueKind::ManifestMismatch => self
                                .record_manifest_mismatch(
                                    database_id,
                                    name.to_string(),
                                    path.to_string(),
                                    match &issue.object_name {
                                        Some(object_name) => {
                                            format!("{}: {}", object_name, issue.detail)
                                        }
                                        None => issue.detail.clone(),
                                    },
                                ),
                        }
                    }
                    if let RecoveryHookResult::Failed { error, .. } = hook_result {
                        self.record_recovery_hook_failure(
                            database_id,
                            name.to_string(),
                            if path.is_empty() {
                                None
                            } else {
                                Some(path.to_string())
                            },
                            error.clone(),
                        );
                    }
                }
                RecoveryHookResult::Reused | RecoveryHookResult::Skipped { .. } => {}
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartupReportCounts {
    pub total: usize,
    pub recovered: usize,
    pub reconciled: usize,
    pub skipped: usize,
    pub failed: usize,
    pub issues: usize,
    pub orphan_directories: usize,
    pub identity_mismatches: usize,
    pub manifest_mismatches: usize,
    pub recovery_hook_failures: usize,
    pub clean_state_invariant_violations: usize,
}
