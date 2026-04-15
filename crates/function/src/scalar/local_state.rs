use std::any::Any;
use std::fmt::Debug;

/// Reusable executor-local state for scalar functions.
pub trait FunctionLocalState: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}
