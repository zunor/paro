// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search-owned canonical property interning.
use super::ids::PropertySetId;
use paro_common::error::Result;
pub use paro_planner::physical::requirements::*;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct PropertyInterner {
    required: Vec<RequiredProperties>,
    required_ids: BTreeMap<RequiredProperties, PropertySetId>,
}

impl PropertyInterner {
    pub fn intern_required(&mut self, properties: RequiredProperties) -> Result<PropertySetId> {
        properties.validate()?;
        if let Some(id) = self.required_ids.get(&properties) {
            return Ok(*id);
        }
        let id = PropertySetId::new(self.required.len());
        self.required.push(properties.clone());
        self.required_ids.insert(properties, id);
        Ok(id)
    }

    pub fn required(&self, id: PropertySetId) -> Option<&RequiredProperties> {
        self.required.get(id.index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interner_is_canonical() {
        let mut interner = PropertyInterner::default();
        let first = interner
            .intern_required(RequiredProperties::default())
            .unwrap();
        let second = interner
            .intern_required(RequiredProperties::default())
            .unwrap();
        assert_eq!(first, second);
    }
}
