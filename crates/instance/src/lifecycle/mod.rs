use self::admission::AdmissionController;
use self::ddl_lock::{InstanceDdlGuard, InstanceDdlLock, InstanceDdlOwner};
use self::startup_report::{StartupPolicy, StartupReport};
use crate::{Instance, ValidChecker};
use std::sync::RwLock;

pub mod admission;
pub mod bootstrap;
pub mod ddl_lock;
pub mod gate;
pub mod recovery;
pub mod shutdown;
pub mod startup_report;

/// Lifecycle control-plane state for an instance.
#[derive(Debug)]
pub struct InstanceLifecycle {
    pub(crate) admission: AdmissionController,
    pub(crate) ddl_lock: InstanceDdlLock,
    pub(crate) boot_id: u64,
    pub(crate) startup_report: RwLock<StartupReport>,
    pub(crate) startup_policy: StartupPolicy,
}

impl InstanceLifecycle {
    pub(crate) fn new(
        startup_policy: StartupPolicy,
        disposition: self::startup_report::InstanceStartupDisposition,
        boot_id: u64,
    ) -> Self {
        Self {
            admission: AdmissionController::new(ValidChecker::new()),
            ddl_lock: InstanceDdlLock::new(),
            boot_id,
            startup_report: RwLock::new(StartupReport::new(startup_policy, disposition)),
            startup_policy,
        }
    }
}

impl Instance {
    pub(crate) fn lock_ddl(
        &self,
        owner: InstanceDdlOwner,
    ) -> paro_common::error::Result<InstanceDdlGuard<'_>> {
        self.lifecycle.admission.check(Some(owner))?;
        let guard = self.lifecycle.ddl_lock.lock(owner);
        self.lifecycle.admission.check(Some(owner))?;
        Ok(guard)
    }

    pub fn startup_report(&self) -> StartupReport {
        self.lifecycle.startup_report.read().unwrap().clone()
    }

    /// Check whether the instance has been invalidated by a fatal lifecycle transition.
    pub fn is_invalidated(&self) -> bool {
        self.lifecycle.admission.is_invalidated()
    }
}
