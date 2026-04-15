// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # PipelineBuilder
//!
//! Simple pipeline builder for unary operator chains.

use crate::operator::PhysicalOperator;
use std::sync::Arc;

use crate::pipeline::pipeline::Pipeline;
use paro_common::error::{self as paro_error, Result};

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn build(root: Arc<dyn PhysicalOperator>) -> Result<Pipeline> {
        let mut chain: Vec<Arc<dyn PhysicalOperator>> = Vec::new();
        let mut current = root;
        loop {
            chain.push(current.clone());
            match current.children_count() {
                0 => break,
                1 => {
                    let child = current.child_arc(0).ok_or_else(|| {
                        paro_error::internal(
                            "Operator reported 1 child but returned None".to_string(),
                        )
                    })?;
                    current = child;
                }
                _ => {
                    return Err(paro_error::not_implemented(
                        "PipelineBuilder currently supports unary operator chains only".to_string(),
                    ));
                }
            }
        }

        let source = chain.last().cloned().ok_or_else(|| {
            paro_error::internal("Cannot build pipeline from empty operator chain".to_string())
        })?;
        if !source.is_source() {
            return Err(paro_error::internal(
                "Leaf operator is not a source".to_string(),
            ));
        }

        let sink = if chain[0].is_sink() {
            Some(chain[0].clone())
        } else {
            None
        };
        let start_idx = if sink.is_some() { 1 } else { 0 };
        let end_idx = chain.len().saturating_sub(1);
        let operators = if start_idx >= end_idx {
            Vec::new()
        } else {
            let mut ops = chain[start_idx..end_idx].to_vec();
            ops.reverse();
            ops
        };

        let pipeline = Pipeline::new();
        pipeline.set_source(source);
        for op in operators {
            pipeline.add_operator(op);
        }
        if let Some(s) = sink {
            pipeline.set_sink(s);
        }
        Ok(pipeline)
    }
}
