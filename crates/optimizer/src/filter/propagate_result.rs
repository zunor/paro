//! Filter simplification outcomes used by statistics propagation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPropagateResult {
    NoPruningPossible,
    /// Filter is always true (can be removed)
    FilterAlwaysTrue,
    /// Filter is always false (replace with empty result)
    FilterAlwaysFalse,
    /// Filter is true or null (can be simplified)
    FilterTrueOrNull,
    /// Filter is false or null (replace with empty result)
    FilterFalseOrNull,
}
