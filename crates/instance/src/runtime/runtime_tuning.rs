use crate::config::InstanceConfigOptions;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTuningSnapshot {
    pub maximum_memory: usize,
    pub maximum_threads: Option<usize>,
    pub use_temporary_directory: bool,
    pub temporary_directory: String,
    pub max_temp_directory_size: Option<usize>,
}

impl RuntimeTuningSnapshot {
    pub fn effective_max_threads(&self) -> usize {
        self.maximum_threads
            .unwrap_or_else(InstanceConfigOptions::get_system_max_threads)
    }

    pub(crate) fn from_options(options: &InstanceConfigOptions) -> Self {
        Self {
            maximum_memory: options.maximum_memory,
            maximum_threads: options.maximum_threads,
            use_temporary_directory: options.use_temporary_directory,
            temporary_directory: options.temporary_directory.clone(),
            max_temp_directory_size: options.max_temp_directory_size,
        }
    }
}

#[derive(Debug)]
pub struct RuntimeTuning {
    inner: RwLock<RuntimeTuningSnapshot>,
}

impl RuntimeTuning {
    pub(crate) fn new(snapshot: RuntimeTuningSnapshot) -> Self {
        Self {
            inner: RwLock::new(snapshot),
        }
    }

    pub(crate) fn from_options(options: &InstanceConfigOptions) -> Self {
        Self::new(RuntimeTuningSnapshot::from_options(options))
    }

    pub fn snapshot(&self) -> RuntimeTuningSnapshot {
        self.inner.read().unwrap().clone()
    }

    pub fn set_maximum_memory(&self, limit: usize) {
        self.inner.write().unwrap().maximum_memory = limit;
    }

    pub fn set_maximum_threads(&self, maximum_threads: Option<usize>) {
        self.inner.write().unwrap().maximum_threads = maximum_threads;
    }

    pub fn set_temporary_directory(&self, path: String) {
        let mut tuning = self.inner.write().unwrap();
        tuning.use_temporary_directory = !path.trim().is_empty();
        tuning.temporary_directory = path;
    }

    pub fn set_max_temp_directory_size(&self, limit: Option<usize>) {
        self.inner.write().unwrap().max_temp_directory_size = limit;
    }
}
