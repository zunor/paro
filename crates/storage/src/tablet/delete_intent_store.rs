use paro_common::error::{self as paro_error, ParoError, Result};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, Clone, Copy)]
struct DeleteIntent {
    txn_id: u64,
    expires_at_ms: u64,
}

#[derive(Debug)]
pub(crate) struct DeleteIntentStore<K: Eq + Hash + Clone> {
    intents: Mutex<HashMap<K, DeleteIntent>>,
    timeout_ms: u64,
}

impl<K: Eq + Hash + Clone> DeleteIntentStore<K> {
    pub(crate) fn new(timeout_ms: u64) -> Self {
        Self {
            intents: Mutex::new(HashMap::new()),
            timeout_ms,
        }
    }

    pub(crate) fn acquire_many<F>(
        &self,
        txn_id: u64,
        keys: &[K],
        now_ms: u64,
        mut conflict_error: F,
    ) -> Result<()>
    where
        F: FnMut(u64, &K) -> ParoError,
    {
        if keys.is_empty() {
            return Ok(());
        }

        let mut intents = self.lock_intents()?;
        Self::prune_expired_locked(&mut intents, now_ms);

        let mut inserted: Vec<K> = Vec::new();
        for key in keys {
            let owner = intents.get(key).copied();
            match owner {
                Some(owner) if owner.txn_id != txn_id => {
                    for inserted_key in inserted {
                        if intents.get(&inserted_key).map(|intent| intent.txn_id) == Some(txn_id) {
                            intents.remove(&inserted_key);
                        }
                    }
                    return Err(conflict_error(owner.txn_id, key));
                }
                Some(_) => {
                    intents.insert(
                        key.clone(),
                        DeleteIntent {
                            txn_id,
                            expires_at_ms: now_ms + self.timeout_ms,
                        },
                    );
                }
                None => {
                    intents.insert(
                        key.clone(),
                        DeleteIntent {
                            txn_id,
                            expires_at_ms: now_ms + self.timeout_ms,
                        },
                    );
                    inserted.push(key.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn release_many(&self, txn_id: u64, keys: &[K]) {
        if keys.is_empty() {
            return;
        }

        let mut intents = match self.intents.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        for key in keys {
            if intents.get(key).map(|intent| intent.txn_id) == Some(txn_id) {
                intents.remove(key);
            }
        }
    }

    pub(crate) fn expire_before(&self, now_ms: u64) {
        if let Ok(mut intents) = self.intents.lock() {
            Self::prune_expired_locked(&mut intents, now_ms);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.intents
            .lock()
            .map(|intents| intents.is_empty())
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(crate) fn force_expire_all(&self) {
        if let Ok(mut intents) = self.intents.lock() {
            for intent in intents.values_mut() {
                intent.expires_at_ms = 0;
            }
        }
    }

    fn lock_intents(&self) -> Result<MutexGuard<'_, HashMap<K, DeleteIntent>>> {
        self.intents
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock delete intents: {e}")))
    }

    fn prune_expired_locked(intents: &mut HashMap<K, DeleteIntent>, now_ms: u64) {
        intents.retain(|_, intent| intent.expires_at_ms > now_ms);
    }
}
