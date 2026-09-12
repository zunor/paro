//! Small independent bag-semantics oracle for the native necessary-domain
//! path.  It intentionally does not call planner code or reuse the Memo's
//! cost/statistics functions: the production adapter must agree with this
//! model on the cases where it claims a safe propagation.

#[derive(Clone, Debug, PartialEq, Eq)]
struct Domain {
    column: usize,
    value: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Predicate {
    Domain(Domain),
    PositiveSum,
}

type Row = Vec<Option<i32>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Plan {
    Scan(Vec<Row>),
    Filter {
        input: Box<Plan>,
        predicates: Vec<Predicate>,
    },
    Projection {
        input: Box<Plan>,
        map: Vec<usize>,
    },
    UnionAll(Box<Plan>, Box<Plan>),
    Aggregate {
        input: Box<Plan>,
        group: usize,
        value_x: usize,
        value_y: usize,
    },
    InnerJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: usize,
        right_key: usize,
    },
    LeftJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: usize,
        right_key: usize,
    },
}

impl Plan {
    fn width(&self) -> usize {
        match self {
            Self::Scan(rows) => rows.first().map_or(0, Vec::len),
            Self::Filter { input, .. } => input.width(),
            Self::Projection { map, .. } => map.len(),
            Self::UnionAll(left, _) => left.width(),
            Self::Aggregate { .. } => 2,
            Self::InnerJoin { left, right, .. } | Self::LeftJoin { left, right, .. } => {
                left.width() + right.width()
            }
        }
    }

    fn rows(&self) -> Vec<Row> {
        match self {
            Self::Scan(rows) => rows.clone(),
            Self::Filter { input, predicates } => input
                .rows()
                .into_iter()
                .filter(|row| {
                    predicates.iter().all(|predicate| match predicate {
                        Predicate::Domain(domain) => {
                            row.get(domain.column).and_then(|value| *value) == Some(domain.value)
                        }
                        Predicate::PositiveSum => row
                            .get(1)
                            .and_then(|value| *value)
                            .is_some_and(|sum| sum > 0),
                    })
                })
                .collect(),
            Self::Projection { input, map } => input
                .rows()
                .into_iter()
                .map(|row| map.iter().map(|index| row[*index]).collect())
                .collect(),
            Self::UnionAll(left, right) => {
                let mut rows = left.rows();
                rows.extend(right.rows());
                rows
            }
            Self::Aggregate {
                input,
                group,
                value_x,
                value_y,
            } => {
                let mut sums = std::collections::BTreeMap::<Option<i32>, Option<i32>>::new();
                for row in input.rows() {
                    let key = row.get(*group).copied().flatten();
                    let value = row
                        .get(*value_x)
                        .copied()
                        .flatten()
                        .zip(row.get(*value_y).copied().flatten())
                        .map(|(x, y)| x - y);
                    let sum = sums.entry(key).or_default();
                    if let Some(value) = value {
                        *sum = Some(sum.unwrap_or(0) + value);
                    }
                }
                sums.into_iter().map(|(key, sum)| vec![key, sum]).collect()
            }
            Self::InnerJoin {
                left,
                right,
                left_key,
                right_key,
            } => join_rows(left.rows(), right.rows(), *left_key, *right_key, false),
            Self::LeftJoin {
                left,
                right,
                left_key,
                right_key,
            } => join_rows(left.rows(), right.rows(), *left_key, *right_key, true),
        }
    }
}

fn join_rows(
    left: Vec<Row>,
    right: Vec<Row>,
    left_key: usize,
    right_key: usize,
    outer: bool,
) -> Vec<Row> {
    let right_width = right.first().map_or(0, Vec::len);
    let mut output = Vec::new();
    for left_row in left {
        let mut matched = false;
        for right_row in &right {
            if left_row.get(left_key).and_then(|value| *value).is_some()
                && left_row.get(left_key).and_then(|value| *value)
                    == right_row.get(right_key).and_then(|value| *value)
            {
                matched = true;
                output.push(left_row.iter().chain(right_row).copied().collect());
            }
        }
        if outer && !matched {
            output.push(
                left_row
                    .iter()
                    .copied()
                    .chain(std::iter::repeat_n(None, right_width))
                    .collect(),
            );
        }
    }
    output
}

fn push_domain(plan: Plan, domain: &Domain) -> (Plan, bool) {
    match plan {
        Plan::Scan(rows) => (
            Plan::Filter {
                input: Box::new(Plan::Scan(rows)),
                predicates: vec![Predicate::Domain(domain.clone())],
            },
            true,
        ),
        Plan::Filter { input, predicates } => {
            let (input, moved) = push_domain(*input, domain);
            (
                Plan::Filter {
                    input: Box::new(input),
                    predicates,
                },
                moved,
            )
        }
        Plan::Projection { input, map } => {
            let Some(child_column) = map.get(domain.column).copied() else {
                return (Plan::Projection { input, map }, false);
            };
            let (input, moved) = push_domain(
                *input,
                &Domain {
                    column: child_column,
                    value: domain.value,
                },
            );
            (
                Plan::Projection {
                    input: Box::new(input),
                    map,
                },
                moved,
            )
        }
        Plan::UnionAll(left, right) => {
            let (left, left_moved) = push_domain(*left, domain);
            let (right, right_moved) = push_domain(*right, domain);
            if left_moved && right_moved {
                (Plan::UnionAll(Box::new(left), Box::new(right)), true)
            } else {
                (Plan::UnionAll(Box::new(left), Box::new(right)), false)
            }
        }
        Plan::Aggregate {
            input,
            group,
            value_x,
            value_y,
        } if domain.column == 0 => {
            let (input, moved) = push_domain(
                *input,
                &Domain {
                    column: group,
                    value: domain.value,
                },
            );
            (
                Plan::Aggregate {
                    input: Box::new(input),
                    group,
                    value_x,
                    value_y,
                },
                moved,
            )
        }
        Plan::Aggregate {
            input,
            group,
            value_x,
            value_y,
        } => (
            Plan::Aggregate {
                input,
                group,
                value_x,
                value_y,
            },
            false,
        ),
        Plan::InnerJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_width = left.width();
            if domain.column < left_width {
                let (left, moved) = push_domain(*left, domain);
                (
                    Plan::InnerJoin {
                        left: Box::new(left),
                        right,
                        left_key,
                        right_key,
                    },
                    moved,
                )
            } else {
                let (right, moved) = push_domain(
                    *right,
                    &Domain {
                        column: domain.column - left_width,
                        value: domain.value,
                    },
                );
                (
                    Plan::InnerJoin {
                        left,
                        right: Box::new(right),
                        left_key,
                        right_key,
                    },
                    moved,
                )
            }
        }
        outer @ Plan::LeftJoin { .. } => (outer, false),
    }
}

fn push_predicates(plan: Plan, predicates: Vec<Predicate>) -> Plan {
    let mut plan = plan;
    let mut residual = Vec::new();
    for predicate in predicates {
        match predicate {
            Predicate::Domain(domain) => {
                let (candidate, moved) = push_domain(plan, &domain);
                plan = candidate;
                if !moved {
                    residual.push(Predicate::Domain(domain));
                }
            }
            other => residual.push(other),
        }
    }
    if residual.is_empty() {
        plan
    } else {
        Plan::Filter {
            input: Box::new(plan),
            predicates: residual,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PropagationKey {
    group: u32,
    domain: u32,
    facts: u64,
    consumer: u32,
}

#[derive(Default)]
struct PropagationOracle {
    completed: std::collections::BTreeSet<PropagationKey>,
    computations: usize,
}

impl PropagationOracle {
    fn request(&mut self, key: PropagationKey) -> bool {
        if self.completed.insert(key) {
            self.computations += 1;
            true
        } else {
            false
        }
    }
}

#[test]
fn exhaustive_bag_cases_keep_union_projection_aggregate_semantics() {
    let left = Plan::Scan(vec![
        vec![Some(2), Some(5), Some(1)],
        vec![Some(2), Some(-5), Some(2)],
        vec![None, Some(9), Some(1)],
    ]);
    let right = Plan::Scan(vec![
        vec![Some(2), Some(3), Some(1)],
        vec![Some(1), None, Some(4)],
    ]);
    let plan = Plan::Projection {
        input: Box::new(Plan::UnionAll(
            Box::new(Plan::Aggregate {
                input: Box::new(left),
                group: 0,
                value_x: 1,
                value_y: 2,
            }),
            Box::new(Plan::Aggregate {
                input: Box::new(right),
                group: 0,
                value_x: 1,
                value_y: 2,
            }),
        )),
        map: vec![0, 1],
    };
    let original = Plan::Filter {
        input: Box::new(plan.clone()),
        predicates: vec![Predicate::Domain(Domain {
            column: 0,
            value: 2,
        })],
    };
    let pushed = push_predicates(
        plan,
        vec![Predicate::Domain(Domain {
            column: 0,
            value: 2,
        })],
    );
    assert_eq!(original.rows(), pushed.rows());

    let residual = Plan::Filter {
        input: Box::new(pushed),
        predicates: vec![Predicate::PositiveSum],
    };
    // The left branch retains its negative SUM(x-y) and is removed only by
    // the output-side PositiveSum predicate; domain propagation must not
    // push that aggregate-result predicate into the input rows.
    assert_eq!(residual.rows(), vec![vec![Some(2), Some(2)]]);
}

#[test]
fn inner_join_is_side_local_but_outer_join_is_a_semantic_barrier() {
    let left = Plan::Scan(vec![vec![Some(1)], vec![Some(2)], vec![None]]);
    let right = Plan::Scan(vec![vec![Some(1)], vec![Some(1)], vec![Some(3)]]);
    let inner = Plan::InnerJoin {
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
        left_key: 0,
        right_key: 0,
    };
    let inner_original = Plan::Filter {
        input: Box::new(inner.clone()),
        predicates: vec![Predicate::Domain(Domain {
            column: 0,
            value: 1,
        })],
    };
    let inner_pushed = push_predicates(
        inner,
        vec![Predicate::Domain(Domain {
            column: 0,
            value: 1,
        })],
    );
    assert_eq!(inner_original.rows(), inner_pushed.rows());

    let outer = Plan::LeftJoin {
        left: Box::new(left),
        right: Box::new(right),
        left_key: 0,
        right_key: 0,
    };
    let (outer_candidate, moved) = push_domain(
        outer.clone(),
        &Domain {
            column: 0,
            value: 2,
        },
    );
    assert!(!moved);
    assert_eq!(outer_candidate, outer.clone());
    assert_eq!(
        push_predicates(
            outer,
            vec![Predicate::Domain(Domain {
                column: 0,
                value: 2
            })]
        )
        .rows(),
        vec![vec![Some(2), None]]
    );
}

#[test]
fn propagation_identity_is_idempotent_and_invalidates_only_changed_facts() {
    let mut oracle = PropagationOracle::default();
    let key = PropagationKey {
        group: 7,
        domain: 11,
        facts: 3,
        consumer: 19,
    };
    assert!(oracle.request(key));
    assert!(!oracle.request(key));
    assert_eq!(oracle.computations, 1);
    assert!(oracle.request(PropagationKey { facts: 4, ..key }));
    assert!(oracle.request(PropagationKey {
        consumer: 20,
        ..key
    }));
    assert_eq!(oracle.computations, 3);
}
