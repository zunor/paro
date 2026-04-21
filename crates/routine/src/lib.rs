// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable routine metadata shared by catalog, planner, runtime, and recovery.

pub mod artifact;
pub mod bound;
pub mod boundary;
pub mod capability;
pub mod env;
pub mod identity;
pub mod permission;
pub mod spec;

pub use artifact::{
    ArtifactCapabilities, ArtifactValidationState, BackendSelectionInput, MinimumIsolation,
    ResolvedEnvArtifact, ResolvedEnvArtifactId, RuntimeContract, TransportKind,
    TrustedBackendPreference,
};
pub use bound::BoundRoutineCallMeta;
pub use boundary::{ExecutionBoundary, PlacementClass, RowSemantics};
pub use capability::{
    CapabilityPolicy, CapabilityProfile, CapabilityProfilePreset, CompiledKernelPolicy,
    NativeJitPolicy, RestrictedSdkProfile, SandboxBackendPreference, SandboxProfile,
    SubInterpreterExtensionPolicy, SubInterpreterGilPolicy, SubInterpreterImportPolicy,
    SubInterpreterPolicy,
};
pub use env::{DeclaredEnvSpec, ImportRef, PackageRequirement, PythonRuntimeSelector};
pub use identity::{
    BuiltinIntrinsicId, BuiltinSemanticTag, RoutineCallIdentity, RoutineId, RoutineIdentity,
};
pub use permission::{PermissionSpec, RoutineSecurityMode};
pub use spec::resolve_best_match;
pub use spec::{
    AggregateRoutineContract, AggregateStateAbi, PythonEntrypointRef, PythonImplementationRef,
    RoutineArgument, RoutineExecutionContract, RoutineFamily, RoutineImplementationRef,
    RoutineNullPolicy, RoutineOwner, RoutineReturn, RoutineSemantics, RoutineSideEffects,
    RoutineSignature, RoutineSpec, RoutineStability, RoutineTableColumn, ScalarRoutineContract,
    SourceBlobRef, TableRoutineContract, WindowRoutineContract,
};
