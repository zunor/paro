use paro_common::runtime_value::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingOverlayChange {
    pub name: String,
    pub previous_value: Option<Value>,
    pub new_value: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionLocalSettings {
    pub(crate) overlay: HashMap<String, Value>,
    pub(crate) journal: Vec<SettingOverlayChange>,
}

impl TransactionLocalSettings {
    pub fn set(&mut self, name: impl Into<String>, value: Option<Value>) {
        let name = name.into().to_lowercase();
        let previous_value = self.overlay.get(&name).cloned();
        self.journal.push(SettingOverlayChange {
            name: name.clone(),
            previous_value,
            new_value: value.clone(),
        });

        match value {
            Some(value) => {
                self.overlay.insert(name, value);
            }
            None => {
                self.overlay.remove(&name);
            }
        }
    }

    pub fn mark(&self) -> usize {
        self.journal.len()
    }

    pub fn rollback_to_mark(&mut self, mark: usize) {
        while self.journal.len() > mark {
            let change = self.journal.pop().expect("journal length already checked");
            match change.previous_value {
                Some(value) => {
                    self.overlay.insert(change.name, value);
                }
                None => {
                    self.overlay.remove(&change.name);
                }
            }
        }
    }
}
