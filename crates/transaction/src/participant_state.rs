// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::types::{ParticipantId, ParticipantKind, TxnResourceKey};
use std::any::Any;
use std::sync::Arc;

/// Opaque participant-local transaction state.
///
/// Storage/catalog/search own their concrete state and downcast only inside
/// their adapter. The transaction-core layer deliberately exposes no write
/// buffer iteration or storage mutation protocol.
pub trait TxnParticipantState: Any + Send + Sync {
    fn participant_id(&self) -> ParticipantId;
    fn participant_kind(&self) -> ParticipantKind;
    fn resource_key(&self) -> TxnResourceKey;
    fn estimated_bytes(&self) -> usize {
        0
    }
    fn as_any(&self) -> &dyn Any;
}

pub type ParticipantStateRef = Arc<dyn TxnParticipantState>;

#[derive(Clone, Default)]
pub struct ParticipantStateSet {
    states: Arc<[ParticipantStateRef]>,
}

impl ParticipantStateSet {
    #[inline]
    pub fn empty() -> Self {
        Self::default()
    }

    #[inline]
    pub fn from_vec(states: Vec<ParticipantStateRef>) -> Self {
        Self {
            states: Arc::from(states.into_boxed_slice()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.states.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &ParticipantStateRef> {
        self.states.iter()
    }
}

impl std::fmt::Debug for ParticipantStateSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParticipantStateSet")
            .field("len", &self.states.len())
            .finish()
    }
}
