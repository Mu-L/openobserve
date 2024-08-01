// Copyright 2024 Zinc Labs Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        Result,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
};
use datafusion_expr::{col, Expr, Limit, LogicalPlan, Sort, SortExpr};

/// Optimization rule that add sort and limit to table scan
#[derive(Default)]
pub struct AddSortAndLimitRule {
    #[allow(dead_code)]
    limit: usize,
}

impl AddSortAndLimitRule {
    #[allow(missing_docs)]
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl OptimizerRule for AddSortAndLimitRule {
    fn name(&self) -> &str {
        "add_sort_and_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let is_complex = plan.exists(|plan| Ok(is_complex_query(plan)))?;
        let mut transformed = match plan {
            LogicalPlan::Projection(mut proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Limit(_) => {
                        // TODO:
                        Transformed::yes(LogicalPlan::Projection(proj))
                    }
                    LogicalPlan::Sort(sort) => {
                        let limit = LogicalPlan::Limit(Limit {
                            skip: 0,
                            fetch: Some(self.limit),
                            input: Arc::new(LogicalPlan::Sort(sort.clone())),
                        });
                        proj.input = Arc::new(limit);
                        Transformed::yes(LogicalPlan::Projection(proj))
                    }
                    _ => {
                        let timestamp = Expr::Sort(SortExpr {
                            expr: Box::new(col("_timestamp")),
                            asc: false,
                            nulls_first: false,
                        });
                        let sort = LogicalPlan::Sort(Sort {
                            expr: vec![timestamp],
                            input: proj.input.clone(),
                            fetch: Some(self.limit),
                        });
                        let limit = LogicalPlan::Limit(Limit {
                            skip: 0,
                            fetch: Some(self.limit),
                            input: Arc::new(sort),
                        });
                        proj.input = Arc::new(limit);
                        Transformed::yes(LogicalPlan::Projection(proj))
                    }
                }
            }
            LogicalPlan::Limit(mut limit) => match limit.input.as_ref() {
                LogicalPlan::Sort(_) => Transformed::yes(LogicalPlan::Limit(limit)),
                _ => {
                    let timestamp = Expr::Sort(SortExpr {
                        expr: Box::new(col("_timestamp")),
                        asc: false,
                        nulls_first: false,
                    });
                    let sort = LogicalPlan::Sort(Sort {
                        expr: vec![timestamp],
                        input: limit.input.clone(),
                        fetch: Some(self.limit),
                    });
                    limit.input = Arc::new(sort);
                    Transformed::yes(LogicalPlan::Limit(limit))
                }
            },
            LogicalPlan::Sort(sort) => {
                let limit = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(self.limit),
                    input: Arc::new(LogicalPlan::Sort(sort)),
                });
                Transformed::yes(limit)
            }
            _ => {
                let timestamp = Expr::Sort(SortExpr {
                    expr: Box::new(col("_timestamp")),
                    asc: false,
                    nulls_first: false,
                });
                let sort = LogicalPlan::Sort(Sort {
                    expr: vec![timestamp],
                    input: Arc::new(plan),
                    fetch: Some(self.limit),
                });
                let limit = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(self.limit),
                    input: Arc::new(sort),
                });
                Transformed::yes(limit)
                // Transformed::no(plan)
            }
        };
        transformed.tnr = TreeNodeRecursion::Stop;
        Ok(transformed)
    }
}

// check if the plan is a complex query that we can't add sort _timestamp
fn is_complex_query(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Subquery(_) => true,
        _ => false,
    }
}

fn construct_limit_plan(plan: LogicalPlan) -> LogicalPlan {
    plan
}

fn construct_limit_and_sort_plan(plan: LogicalPlan) -> LogicalPlan {
    plan
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        arrow::record_batch::RecordBatch, assert_batches_eq, datasource::MemTable,
        prelude::SessionContext,
    };

    use super::AddSortAndLimitRule;

    #[tokio::test]
    async fn test_real_sql_for_timestamp() {
        let sqls = [
            (
                "select name from t order by _timestamp ASC",
                vec![
                    "+-------------+",
                    "| name        |",
                    "+-------------+",
                    "| openobserve |",
                    "| observe     |",
                    "+-------------+",
                ],
            ),
            (
                "select * from t",
                vec![
                    "+------------+------+",
                    "| _timestamp | name |",
                    "+------------+------+",
                    "| 5          | o2   |",
                    "| 4          | oo   |",
                    "+------------+------+",
                ],
            ),
            (
                "select * from t where _timestamp > 2 and name != 'oo'",
                vec![
                    "+------------+-------------+",
                    "| _timestamp | name        |",
                    "+------------+-------------+",
                    "| 5          | o2          |",
                    "| 3          | openobserve |",
                    "+------------+-------------+",
                ],
            ),
            (
                "select count(*) from t",
                vec![
                    "+----------+",
                    "| count(*) |",
                    "+----------+",
                    "| 5        |",
                    "+----------+",
                ],
            ),
            (
                "select name, count(*) from t group by name order by name",
                vec![
                    "+----------+",
                    "| count(*) |",
                    "+----------+",
                    "| 5        |",
                    "+----------+",
                ],
            ),
        ];

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "openobserve",
                    "observe",
                    "openobserve",
                    "oo",
                    "o2",
                ])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.add_optimizer_rule(Arc::new(AddSortAndLimitRule::new(2)));

        for item in sqls {
            let df = ctx.sql(item.0).await.unwrap();
            let data = df.collect().await.unwrap();
            assert_batches_eq!(item.1, &data);
        }
    }
}
