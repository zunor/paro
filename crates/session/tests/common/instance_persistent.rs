use std::path::Path;
use std::sync::Arc;

use paro_instance::{Instance, InstanceConfig};

pub fn create_persistent_instance(base_dir: &Path) -> Arc<Instance> {
    let config = InstanceConfig::new().with_instance_root(base_dir.to_string_lossy().to_string());
    Instance::new(config).expect("failed to create persistent instance")
}
