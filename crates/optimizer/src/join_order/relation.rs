// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join-relation sets used by the join-order optimizer.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

/// A set of relations (tables) involved in a join.
///
/// Relations are stored as a sorted array of indices for efficient
/// subset checking and union operations.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct JoinRelationSet {
    /// Sorted array of relation indices.
    relations: Vec<usize>,
}

impl JoinRelationSet {
    /// Create a new JoinRelationSet from a sorted, duplicate-free list of relations.
    pub fn new(relations: Vec<usize>) -> Self {
        debug_assert!(
            Self::is_sorted_unique(&relations),
            "Relations must be sorted and unique"
        );
        Self { relations }
    }

    /// Create a JoinRelationSet from a single relation index.
    pub fn single(index: usize) -> Self {
        Self {
            relations: vec![index],
        }
    }

    /// Create an empty JoinRelationSet.
    pub fn empty() -> Self {
        Self {
            relations: Vec::new(),
        }
    }

    /// Get the number of relations in this set.
    pub fn count(&self) -> usize {
        self.relations.len()
    }

    /// Check if this set is empty.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Get the relations as a slice.
    pub fn relations(&self) -> &[usize] {
        &self.relations
    }

    /// Check if this set contains a specific relation.
    pub fn contains(&self, relation: usize) -> bool {
        self.relations.binary_search(&relation).is_ok()
    }

    /// Check if `sub` is a subset of `super_set`.
    ///
    /// Both sets must be sorted for this to work correctly.
    pub fn is_subset(super_set: &JoinRelationSet, sub: &JoinRelationSet) -> bool {
        if sub.count() == 0 {
            return true;
        }
        if sub.count() > super_set.count() {
            return false;
        }

        let mut j = 0;
        for i in 0..super_set.count() {
            if sub.relations[j] == super_set.relations[i] {
                j += 1;
                if j == sub.count() {
                    return true;
                }
            }
        }
        false
    }

    /// Check if two sets are disjoint (have no common elements).
    pub fn is_disjoint(&self, other: &JoinRelationSet) -> bool {
        let mut i = 0;
        let mut j = 0;
        while i < self.count() && j < other.count() {
            if self.relations[i] == other.relations[j] {
                return false;
            } else if self.relations[i] < other.relations[j] {
                i += 1;
            } else {
                j += 1;
            }
        }
        true
    }

    /// Compute the union of two sets.
    pub fn union(&self, other: &JoinRelationSet) -> JoinRelationSet {
        let mut result = Vec::with_capacity(self.count() + other.count());
        let mut i = 0;
        let mut j = 0;

        while i < self.count() && j < other.count() {
            if self.relations[i] < other.relations[j] {
                result.push(self.relations[i]);
                i += 1;
            } else if self.relations[i] > other.relations[j] {
                result.push(other.relations[j]);
                j += 1;
            } else {
                // Equal - add once
                result.push(self.relations[i]);
                i += 1;
                j += 1;
            }
        }

        // Add remaining elements
        while i < self.count() {
            result.push(self.relations[i]);
            i += 1;
        }
        while j < other.count() {
            result.push(other.relations[j]);
            j += 1;
        }

        JoinRelationSet::new(result)
    }

    /// Compute the intersection of two sets.
    pub fn intersection(&self, other: &JoinRelationSet) -> JoinRelationSet {
        let mut result = Vec::new();
        let mut i = 0;
        let mut j = 0;

        while i < self.count() && j < other.count() {
            if self.relations[i] == other.relations[j] {
                result.push(self.relations[i]);
                i += 1;
                j += 1;
            } else if self.relations[i] < other.relations[j] {
                i += 1;
            } else {
                j += 1;
            }
        }

        JoinRelationSet::new(result)
    }

    /// Compute the difference of two sets (self - other).
    pub fn difference(&self, other: &JoinRelationSet) -> JoinRelationSet {
        let mut result = Vec::new();
        let mut i = 0;
        let mut j = 0;

        while i < self.count() {
            if j >= other.count() {
                // Add remaining from self
                result.push(self.relations[i]);
                i += 1;
            } else if self.relations[i] == other.relations[j] {
                // Skip common elements
                i += 1;
                j += 1;
            } else if self.relations[i] < other.relations[j] {
                result.push(self.relations[i]);
                i += 1;
            } else {
                j += 1;
            }
        }

        JoinRelationSet::new(result)
    }

    /// Check if a vector is sorted and contains no duplicates.
    fn is_sorted_unique(v: &[usize]) -> bool {
        for i in 1..v.len() {
            if v[i - 1] >= v[i] {
                return false;
            }
        }
        true
    }
}

impl fmt::Display for JoinRelationSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, rel) in self.relations.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", rel)?;
        }
        write!(f, "]")
    }
}

impl fmt::Debug for JoinRelationSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinRelationSet{}", self)
    }
}

/// Node in the JoinRelationSet tree.
struct JoinRelationTreeNode {
    /// The JoinRelationSet at this node (if any).
    relation: Option<Arc<JoinRelationSet>>,
    /// Child nodes indexed by relation index.
    children: HashMap<usize, Box<JoinRelationTreeNode>>,
}

impl JoinRelationTreeNode {
    fn new() -> Self {
        Self {
            relation: None,
            children: HashMap::new(),
        }
    }
}

/// Manager for creating and looking up JoinRelationSets.
///
/// Uses a tree structure to efficiently deduplicate sets.
/// Each path from root to a node represents a sorted sequence of relation indices.
pub struct JoinRelationSetManager {
    /// Root of the tree.
    root: JoinRelationTreeNode,
}

impl JoinRelationSetManager {
    /// Create a new JoinRelationSetManager.
    pub fn new() -> Self {
        Self {
            root: JoinRelationTreeNode::new(),
        }
    }

    /// Get or create a JoinRelationSet from a single relation index.
    pub fn get_relation(&mut self, index: usize) -> Arc<JoinRelationSet> {
        self.get_relation_from_vec(vec![index])
    }

    /// Get or create a JoinRelationSet from a set of relation bindings.
    pub fn get_relation_from_set(&mut self, bindings: &HashSet<usize>) -> Arc<JoinRelationSet> {
        let mut relations: Vec<usize> = bindings.iter().copied().collect();
        relations.sort_unstable();
        self.get_relation_from_vec(relations)
    }

    /// Get or create a JoinRelationSet from a sorted, duplicate-free vector.
    pub fn get_relation_from_vec(&mut self, relations: Vec<usize>) -> Arc<JoinRelationSet> {
        // Navigate/create the tree path
        let mut node = &mut self.root;
        for &rel in &relations {
            node = node
                .children
                .entry(rel)
                .or_insert_with(|| Box::new(JoinRelationTreeNode::new()));
        }

        // Get or create the JoinRelationSet
        if node.relation.is_none() {
            node.relation = Some(Arc::new(JoinRelationSet::new(relations)));
        }
        node.relation.clone().unwrap()
    }

    /// Union two JoinRelationSets and return the result.
    pub fn union(
        &mut self,
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> Arc<JoinRelationSet> {
        let result = left.union(right);
        self.get_relation_from_vec(result.relations)
    }

    fn render_sets(&self) -> String {
        let mut result = String::new();
        Self::node_to_string(&self.root, &mut result);
        result
    }

    fn node_to_string(node: &JoinRelationTreeNode, result: &mut String) {
        if let Some(ref relation) = node.relation {
            result.push_str(&format!("{}\n", relation));
        }
        for child in node.children.values() {
            Self::node_to_string(child, result);
        }
    }
}

impl Default for JoinRelationSetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for JoinRelationSetManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinRelationSetManager{{\n{}}}", self.render_sets())
    }
}

impl fmt::Display for JoinRelationSetManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render_sets())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_relation_set_single() {
        let set = JoinRelationSet::single(5);
        assert_eq!(set.count(), 1);
        assert!(set.contains(5));
        assert!(!set.contains(3));
        assert_eq!(set.to_string(), "[5]");
    }

    #[test]
    fn test_join_relation_set_multiple() {
        let set = JoinRelationSet::new(vec![1, 3, 5, 7]);
        assert_eq!(set.count(), 4);
        assert!(set.contains(1));
        assert!(set.contains(3));
        assert!(set.contains(5));
        assert!(set.contains(7));
        assert!(!set.contains(2));
        assert!(!set.contains(4));
        assert_eq!(set.to_string(), "[1, 3, 5, 7]");
    }

    #[test]
    fn test_join_relation_set_empty() {
        let set = JoinRelationSet::empty();
        assert_eq!(set.count(), 0);
        assert!(set.is_empty());
        assert_eq!(set.to_string(), "[]");
    }

    #[test]
    fn test_is_subset() {
        let super_set = JoinRelationSet::new(vec![1, 2, 3, 4, 5]);
        let sub1 = JoinRelationSet::new(vec![2, 4]);
        let sub2 = JoinRelationSet::new(vec![1, 3, 5]);
        let not_sub = JoinRelationSet::new(vec![2, 6]);
        let empty = JoinRelationSet::empty();

        assert!(JoinRelationSet::is_subset(&super_set, &sub1));
        assert!(JoinRelationSet::is_subset(&super_set, &sub2));
        assert!(!JoinRelationSet::is_subset(&super_set, &not_sub));
        assert!(JoinRelationSet::is_subset(&super_set, &empty));
        assert!(JoinRelationSet::is_subset(&super_set, &super_set));
    }

    #[test]
    fn test_is_disjoint() {
        let set1 = JoinRelationSet::new(vec![1, 3, 5]);
        let set2 = JoinRelationSet::new(vec![2, 4, 6]);
        let set3 = JoinRelationSet::new(vec![3, 4, 5]);

        assert!(set1.is_disjoint(&set2));
        assert!(!set1.is_disjoint(&set3));
        assert!(!set2.is_disjoint(&set3));
    }

    #[test]
    fn test_union() {
        let set1 = JoinRelationSet::new(vec![1, 3, 5]);
        let set2 = JoinRelationSet::new(vec![2, 3, 4]);
        let result = set1.union(&set2);

        assert_eq!(result.count(), 5);
        assert_eq!(result.relations(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_union_disjoint() {
        let set1 = JoinRelationSet::new(vec![1, 2]);
        let set2 = JoinRelationSet::new(vec![3, 4]);
        let result = set1.union(&set2);

        assert_eq!(result.count(), 4);
        assert_eq!(result.relations(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_union_with_empty() {
        let set1 = JoinRelationSet::new(vec![1, 2, 3]);
        let empty = JoinRelationSet::empty();

        let result1 = set1.union(&empty);
        let result2 = empty.union(&set1);

        assert_eq!(result1.relations(), &[1, 2, 3]);
        assert_eq!(result2.relations(), &[1, 2, 3]);
    }

    #[test]
    fn test_intersection() {
        let set1 = JoinRelationSet::new(vec![1, 2, 3, 4]);
        let set2 = JoinRelationSet::new(vec![2, 4, 6]);
        let result = set1.intersection(&set2);

        assert_eq!(result.count(), 2);
        assert_eq!(result.relations(), &[2, 4]);
    }

    #[test]
    fn test_intersection_disjoint() {
        let set1 = JoinRelationSet::new(vec![1, 3, 5]);
        let set2 = JoinRelationSet::new(vec![2, 4, 6]);
        let result = set1.intersection(&set2);

        assert!(result.is_empty());
    }

    #[test]
    fn test_difference() {
        let set1 = JoinRelationSet::new(vec![1, 2, 3, 4, 5]);
        let set2 = JoinRelationSet::new(vec![2, 4]);
        let result = set1.difference(&set2);

        assert_eq!(result.count(), 3);
        assert_eq!(result.relations(), &[1, 3, 5]);
    }

    #[test]
    fn test_difference_no_overlap() {
        let set1 = JoinRelationSet::new(vec![1, 3, 5]);
        let set2 = JoinRelationSet::new(vec![2, 4, 6]);
        let result = set1.difference(&set2);

        assert_eq!(result.relations(), &[1, 3, 5]);
    }

    #[test]
    fn test_manager_get_single() {
        let mut manager = JoinRelationSetManager::new();
        let set1 = manager.get_relation(5);
        let set2 = manager.get_relation(5);

        // Should return the same Arc
        assert!(Arc::ptr_eq(&set1, &set2));
        assert_eq!(set1.count(), 1);
        assert!(set1.contains(5));
    }

    #[test]
    fn test_manager_get_from_set() {
        let mut manager = JoinRelationSetManager::new();
        let bindings: HashSet<usize> = [1, 3, 5].into_iter().collect();
        let set1 = manager.get_relation_from_set(&bindings);
        let set2 = manager.get_relation_from_set(&bindings);

        // Should return the same Arc
        assert!(Arc::ptr_eq(&set1, &set2));
        assert_eq!(set1.count(), 3);
        assert_eq!(set1.relations(), &[1, 3, 5]);
    }

    #[test]
    fn test_manager_get_from_vec() {
        let mut manager = JoinRelationSetManager::new();
        let set1 = manager.get_relation_from_vec(vec![2, 4, 6]);
        let set2 = manager.get_relation_from_vec(vec![2, 4, 6]);

        // Should return the same Arc
        assert!(Arc::ptr_eq(&set1, &set2));
        assert_eq!(set1.count(), 3);
    }

    #[test]
    fn test_manager_union() {
        let mut manager = JoinRelationSetManager::new();
        let left = manager.get_relation_from_vec(vec![1, 3]);
        let right = manager.get_relation_from_vec(vec![2, 3, 4]);
        let result = manager.union(&left, &right);

        assert_eq!(result.count(), 4);
        assert_eq!(result.relations(), &[1, 2, 3, 4]);

        // Getting the same union again should return the same Arc
        let result2 = manager.union(&left, &right);
        assert!(Arc::ptr_eq(&result, &result2));
    }

    #[test]
    fn test_manager_different_sets() {
        let mut manager = JoinRelationSetManager::new();
        let set1 = manager.get_relation_from_vec(vec![1, 2]);
        let set2 = manager.get_relation_from_vec(vec![1, 3]);
        let set3 = manager.get_relation_from_vec(vec![2, 3]);

        // All should be different
        assert!(!Arc::ptr_eq(&set1, &set2));
        assert!(!Arc::ptr_eq(&set1, &set3));
        assert!(!Arc::ptr_eq(&set2, &set3));
    }

    #[test]
    fn test_manager_to_string() {
        let mut manager = JoinRelationSetManager::new();
        manager.get_relation(1);
        manager.get_relation_from_vec(vec![1, 2]);
        manager.get_relation_from_vec(vec![2, 3]);

        let s = manager.to_string();
        assert!(s.contains("[1]"));
        assert!(s.contains("[1, 2]"));
        assert!(s.contains("[2, 3]"));
    }

    #[test]
    fn test_default_manager_is_empty() {
        assert_eq!(JoinRelationSetManager::default().to_string(), "");
    }
}
