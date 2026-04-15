use std::sync::{Arc, OnceLock, RwLock};

use paro_storage::buffer::BufferManager;

fn registry() -> &'static RwLock<Option<Arc<dyn BufferManager>>> {
    static REGISTRY: OnceLock<RwLock<Option<Arc<dyn BufferManager>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(None))
}

/// Register the buffer manager used by system memory table functions.
pub fn register_system_buffer_manager(buffer_manager: Arc<dyn BufferManager>) {
    let mut guard = registry().write().unwrap();
    *guard = Some(buffer_manager);
}

/// Get the currently registered buffer manager.
pub fn get_system_buffer_manager() -> Option<Arc<dyn BufferManager>> {
    registry().read().unwrap().clone()
}
