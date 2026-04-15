//! Plan Filter - Convert Filter to Filter
//!
//!

use super::generator::PhysicalPlanGenerator;
use super::predicate_builder;
use crate::operator::filter::Filter as PhysicalFilter;
use crate::operator::scan::fulltext_scan::FullTextQueryKind;
use crate::operator::scan::rowset_scan::{PhysicalRowsetScan, RowsetScanBindData};
use crate::operator::PhysicalOperator;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, IndexType as CatalogIndexType, TableCatalogEntry,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
use paro_planner::operator::LogicalOperator;
use paro_planner::operator::{Filter as LogicalFilter, Get};
use paro_storage::index::fulltext::tokenizer::TokenizerKind;
use paro_storage::table::table_handle::TableHandle;

use std::sync::Arc;

const SIMPLE_CONFIG: &str = "simple";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextMatchInfo {
    pub(crate) text_column_id: usize,
    pub(crate) query_text: String,
    pub(crate) query_kind: FullTextQueryKind,
    pub(crate) config: String,
}

impl PhysicalPlanGenerator {
    /// Create physical plan for Filter.
    pub fn create_plan_filter(
        &self,
        filter: &LogicalFilter,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if filter.expressions.is_empty() {
            return Ok(child);
        }

        // Try to push down filters to RowsetScan
        if let LogicalOperator::Get(get) = &filter.child.operator {
            let (predicate_tree, mut residual) =
                predicate_builder::build_predicate_tree(&filter.expressions, get)?;
            let (runtime_tree, mut runtime_residual) =
                predicate_builder::build_predicate_tree(&get.runtime_filter_expressions, get)?;
            let predicate_tree =
                predicate_builder::combine_predicate_trees(predicate_tree, runtime_tree);
            residual.append(&mut runtime_residual);

            if let Some(tree) = predicate_tree {
                // We have something to push down
                let table_entry = get
                    .get_table()
                    .ok_or_else(|| paro_error::internal("Get missing table reference"))?;
                let table_data = table_entry
                    .get_storage()
                    .ok_or_else(|| paro_error::internal("Table has no storage"))?
                    .clone();

                let physical_cols = table_data.types().len();
                let mut emit_row_id = false;
                let projected_columns = if get.column_ids.is_empty() {
                    Vec::new()
                } else {
                    let mut cols = Vec::new();
                    for &col_id in &get.column_ids {
                        if col_id < physical_cols {
                            cols.push(col_id);
                        } else if col_id == physical_cols {
                            emit_row_id = true;
                        } else {
                            return Err(paro_error::invalid_input(format!(
                                "Get column_id {} out of range (physical columns: {})",
                                col_id, physical_cols
                            )));
                        }
                    }
                    cols
                };

                let mut bind_data = if get.column_ids.is_empty() {
                    RowsetScanBindData::from_table_data(table_data)
                } else {
                    RowsetScanBindData::from_table_data_with_projection(
                        table_data,
                        projected_columns,
                    )
                }
                .with_output_types(get.returned_types.clone())
                .with_emit_row_id(emit_row_id)
                .with_relation(get.relation_name.clone(), get.relation_alias.clone());
                bind_data = bind_data.with_predicate(tree);

                let scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
                    Arc::new(PhysicalRowsetScan::new(bind_data)),
                    crate::explain::types::ExplainSchema {
                        output_names: get.names.clone(),
                        relation_name: get.relation_name.clone(),
                        relation_alias: get.relation_alias.clone(),
                    },
                );
                let mut current_op: Arc<dyn PhysicalOperator> = scan;

                // If there are residuals, add a Filter on top
                if !residual.is_empty() {
                    let predicate = if residual.len() == 1 {
                        residual[0].clone()
                    } else {
                        Expression::Conjunction(ConjunctionExpression {
                            conjunction_type: ConjunctionType::And,
                            children: residual,
                        })
                    };
                    let filter_op: Arc<dyn PhysicalOperator> =
                        Arc::new(PhysicalFilter::new(predicate, current_op.clone()));
                    current_op = self.annotate_schema(
                        filter_op,
                        self.passthrough_schema(&current_op, filter.child.output_names()),
                    );
                }

                return Ok(current_op);
            }
        }

        // Fallback: Use Filter operator
        let predicate = if filter.expressions.len() == 1 {
            filter.expressions[0].clone()
        } else {
            // Combine multiple expressions into an AND conjunction
            Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: filter.expressions.clone(),
            })
        };

        let physical_filter = if filter.projection_map.is_empty() {
            PhysicalFilter::new(predicate, child)
        } else {
            PhysicalFilter::with_projection_map(predicate, filter.projection_map.clone(), child)
        };
        Ok(Arc::new(physical_filter))
    }
}

pub(crate) fn fulltext_index_pushdown_ready(
    generator: &PhysicalPlanGenerator,
    table_entry: &TableCatalogEntry,
    table_data: &TableHandle,
    info: &FullTextMatchInfo,
) -> bool {
    let runtime_coverage = match table_data.fulltext_index_coverage(info.text_column_id as u32) {
        Ok(coverage) => coverage,
        Err(_) => return false,
    };
    if !runtime_coverage.is_complete() {
        return false;
    }

    let txn = generator.context.catalog_txn_view();
    let catalog = generator.context.catalog();
    let schema = match catalog.get_schema(&txn, &table_entry.base.schema_name) {
        Ok(schema) => schema,
        Err(_) => return false,
    };

    for entry in schema
        .collection(CatalogType::Index)
        .expect("index collection")
        .scan(txn.transaction_id, txn.start_time)
    {
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            continue;
        };
        if index.table_oid != table_entry.base.base.object_id.raw() {
            continue;
        }
        if index.index_type != CatalogIndexType::FullText || !index.is_ready() {
            continue;
        }
        let Some(binding) = index.fulltext_binding() else {
            continue;
        };
        if binding.column_id.index != info.text_column_id as u32 {
            continue;
        }
        if !binding.config.eq_ignore_ascii_case(&info.config) {
            continue;
        }
        return true;
    }

    false
}

pub(crate) fn extract_fulltext_match(
    expr: &Expression,
    get: &Get,
) -> Result<Option<FullTextMatchInfo>> {
    let expr = strip_casts(expr);

    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let name = func.function.name.to_lowercase();
    if !is_fulltext_match_function(&name) {
        return Ok(None);
    }
    match name.as_str() {
        "fulltext_match" => extract_legacy_fulltext_match(func, get),
        "fulltext_match_internal" => extract_internal_fulltext_match(func, get),
        _ => Ok(None),
    }
}

fn is_fulltext_match_function(name: &str) -> bool {
    matches!(name, "fulltext_match" | "fulltext_match_internal")
}

fn extract_legacy_fulltext_match(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
) -> Result<Option<FullTextMatchInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(col_id) = resolve_fulltext_column(get, extract_scan_col_idx(left)) {
        if let Some(query) = extract_query_string(right)? {
            return Ok(Some(FullTextMatchInfo {
                text_column_id: col_id,
                query_text: query,
                query_kind: FullTextQueryKind::Legacy,
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    if let Some(col_id) = resolve_fulltext_column(get, extract_scan_col_idx(right)) {
        if let Some(query) = extract_query_string(left)? {
            return Ok(Some(FullTextMatchInfo {
                text_column_id: col_id,
                query_text: query,
                query_kind: FullTextQueryKind::Legacy,
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    Ok(None)
}

fn extract_internal_fulltext_match(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
) -> Result<Option<FullTextMatchInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }

    let Some((text_column_id, tsv_config)) = extract_tsvector_source(&func.children[0], get)?
    else {
        return Ok(None);
    };
    let Some((query_text, query_kind, tsq_config)) = extract_tsquery_source(&func.children[1])?
    else {
        return Ok(None);
    };

    if !tsv_config.eq_ignore_ascii_case(&tsq_config) {
        return Ok(None);
    }

    Ok(Some(FullTextMatchInfo {
        text_column_id,
        query_text,
        query_kind,
        config: tsv_config,
    }))
}

fn extract_tsvector_source(expr: &Expression, get: &Get) -> Result<Option<(usize, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };
    if !func.function.name.eq_ignore_ascii_case("to_tsvector") {
        return Ok(None);
    }

    let (config_expr, text_expr) = match func.children.as_slice() {
        [text] => (None, text),
        [config, text] => (Some(config), text),
        _ => return Ok(None),
    };
    let config = match config_expr {
        Some(config_expr) => match extract_query_string(config_expr)? {
            Some(config) => {
                let Some(normalized) = normalize_fulltext_config(&config) else {
                    return Ok(None);
                };
                normalized
            }
            None => return Ok(None),
        },
        None => SIMPLE_CONFIG.to_string(),
    };
    let col_id = resolve_fulltext_column(get, extract_scan_col_idx(text_expr));
    Ok(col_id.map(|id| (id, config)))
}

fn extract_tsquery_source(
    expr: &Expression,
) -> Result<Option<(String, FullTextQueryKind, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let (query_kind, allow_single_arg_default_config) =
        match func.function.name.to_ascii_lowercase().as_str() {
            "to_tsquery" => (FullTextQueryKind::TsQuery, false),
            "plainto_tsquery" => (FullTextQueryKind::Plain, true),
            "phraseto_tsquery" => (FullTextQueryKind::Phrase, false),
            "websearch_to_tsquery" => (FullTextQueryKind::WebSearch, false),
            _ => return Ok(None),
        };

    let (config, query_expr) = match func.children.as_slice() {
        [config, query] => {
            let Some(cfg) = extract_query_string(config)? else {
                return Ok(None);
            };
            let Some(normalized) = normalize_fulltext_config(&cfg) else {
                return Ok(None);
            };
            (normalized, query)
        }
        [query] if allow_single_arg_default_config => (SIMPLE_CONFIG.to_string(), query),
        _ => return Ok(None),
    };

    let Some(query_text) = extract_query_string(query_expr)? else {
        return Ok(None);
    };
    Ok(Some((query_text, query_kind, config)))
}

fn extract_scan_col_idx(expr: &Expression) -> Option<usize> {
    predicate_builder::extract_scan_column_index(strip_casts(expr))
}

fn resolve_fulltext_column(get: &Get, col_idx: Option<usize>) -> Option<usize> {
    let col_idx = col_idx?;
    if col_idx >= get.column_ids.len() || col_idx >= get.column_types.len() {
        return None;
    }
    if !matches!(get.column_types[col_idx], LogicalType::Varchar) {
        return None;
    }
    Some(get.column_ids[col_idx])
}

fn strip_casts(mut expr: &Expression) -> &Expression {
    while let Expression::Cast(cast) = expr {
        expr = cast.child.as_ref();
    }
    expr
}

fn normalize_fulltext_config(config: &str) -> Option<String> {
    TokenizerKind::from_config(config)
        .ok()
        .map(|kind| kind.config_name().to_string())
}

fn extract_query_string(expr: &Expression) -> Result<Option<String>> {
    match expr {
        Expression::Constant(c) => {
            if let Value::Varchar(s) = &c.value {
                return Ok(Some(s.clone()));
            }
            Ok(None)
        }
        Expression::Cast(cast) => extract_query_string(cast.child.as_ref()),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_planner::expression::{
        CastExpression, ConstantExpression, FunctionExpression, ReferenceExpression,
    };
    use paro_planner::operator::get::Get;

    fn dummy_fn(
        _: &Chunk,
        _: &dyn paro_function::scalar::ExpressionState,
        _: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    fn make_fulltext_expr(function_name: &str) -> Expression {
        let args = if function_name == "fulltext_match_internal" {
            vec![LogicalType::TsVector, LogicalType::TsQuery]
        } else {
            vec![LogicalType::Varchar, LogicalType::Varchar]
        };
        Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                function_name.to_string(),
                args,
                LogicalType::Boolean,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("query".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ))
    }

    #[test]
    fn test_extract_fulltext_match_legacy_name() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        let expr = make_fulltext_expr("fulltext_match");

        let res = extract_fulltext_match(&expr, &get).unwrap();
        assert_eq!(
            res,
            Some(FullTextMatchInfo {
                text_column_id: 0,
                query_text: "query".to_string(),
                query_kind: FullTextQueryKind::Legacy,
                config: "simple".to_string(),
            })
        );
    }

    #[test]
    fn test_extract_fulltext_match_internal_name() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        let to_tsvector = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "to_tsvector".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsVector,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
            ],
            LogicalType::TsVector,
        ));
        let plainto = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "plainto_tsquery".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsQuery,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("vector db".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::TsQuery,
        ));
        let expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "fulltext_match_internal".to_string(),
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Boolean,
                dummy_fn,
            ),
            vec![to_tsvector, plainto],
            LogicalType::Boolean,
        ));

        let res = extract_fulltext_match(&expr, &get).unwrap();
        assert_eq!(
            res,
            Some(FullTextMatchInfo {
                text_column_id: 0,
                query_text: "vector db".to_string(),
                query_kind: FullTextQueryKind::Plain,
                config: "simple".to_string(),
            })
        );
    }

    #[test]
    fn test_extract_fulltext_match_internal_with_cast_wrappers() {
        use paro_function::scalar::cast::BoundCastInfo;

        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        let to_tsvector = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "to_tsvector".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsVector,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
            ],
            LogicalType::TsVector,
        ));
        let tsquery = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "to_tsquery".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsQuery,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("vector & database".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::TsQuery,
        ));
        let wrapped_tsv = Expression::Cast(CastExpression::new(
            to_tsvector,
            LogicalType::TsVector,
            BoundCastInfo::identity(&LogicalType::TsVector, &LogicalType::TsVector),
            false,
        ));
        let wrapped_tsq = Expression::Cast(CastExpression::new(
            tsquery,
            LogicalType::TsQuery,
            BoundCastInfo::identity(&LogicalType::TsQuery, &LogicalType::TsQuery),
            false,
        ));
        let expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "fulltext_match_internal".to_string(),
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Boolean,
                dummy_fn,
            ),
            vec![wrapped_tsv, wrapped_tsq],
            LogicalType::Boolean,
        ));

        let res = extract_fulltext_match(&expr, &get).unwrap();
        assert_eq!(
            res,
            Some(FullTextMatchInfo {
                text_column_id: 0,
                query_text: "vector & database".to_string(),
                query_kind: FullTextQueryKind::TsQuery,
                config: "simple".to_string(),
            })
        );
    }

    #[test]
    fn test_extract_fulltext_match_rejects_other_functions() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        let expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "contains".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Boolean,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("query".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ));

        let res = extract_fulltext_match(&expr, &get).unwrap();
        assert!(res.is_none());
    }
}
