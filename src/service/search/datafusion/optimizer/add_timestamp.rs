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
    common::{tree_node::Transformed, Result},
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
};
use datafusion_expr::{and, col, lit, Expr, Filter, LogicalPlan};

/// Optimization rule that add _timestamp constraint to table scan
///
/// Note: should apply before push down filter rule
#[derive(Default)]
pub struct AddTimestampRule {
    filter: Expr,
}

impl AddTimestampRule {
    #[allow(missing_docs)]
    pub fn new(start_time: i64, end_time: i64) -> Self {
        Self {
            filter: and(
                col("_timestamp").gt_eq(lit(start_time)),
                col("_timestamp").lt(lit(end_time)),
            ),
        }
    }
}

impl OptimizerRule for AddTimestampRule {
    fn name(&self) -> &str {
        "add_timestamp"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(_) => {
                let filter_plan =
                    LogicalPlan::Filter(Filter::try_new(self.filter.clone(), Arc::new(plan))?);
                Ok(Transformed::yes(filter_plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        common::{Column, Result},
        optimizer::{Optimizer, OptimizerContext, OptimizerRule},
    };
    use datafusion_expr::{
        and, binary_expr, col, in_subquery, lit, table_scan, JoinType, LogicalPlan,
        LogicalPlanBuilder, Operator,
    };

    use super::AddTimestampRule;

    fn test_table() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_timestamp", DataType::Int64, false),
        ]);
        let test = table_scan(Some("test"), &schema, None)?.build()?;
        Ok(test)
    }

    fn create_table_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_timestamp", DataType::Int64, false),
        ]);
        let test = table_scan(Some(name), &schema, None)?.build()?;
        Ok(test)
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(AddTimestampRule::new(0, 5)), plan, expected)
    }

    // from datafusion
    pub fn assert_optimized_plan_eq(
        rule: Arc<dyn OptimizerRule + Send + Sync>,
        plan: LogicalPlan,
        expected: &str,
    ) -> Result<()> {
        // Apply the rule once
        let opt_context = OptimizerContext::new().with_max_passes(1);

        let optimizer = Optimizer::with_rules(vec![Arc::clone(&rule)]);
        let optimized_plan = optimizer.optimize(plan, &opt_context, observe)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        Ok(())
    }

    #[test]
    fn test_table_scan() -> Result<()> {
        let table_scan = test_table()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;

        let expected = "Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n  TableScan: test";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_table_scan_with_projection() -> Result<()> {
        let table_scan = test_table()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("id")])?
            .build()?;

        let expected = "Projection: test.id\
        \n  Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n    TableScan: test";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_table_scan_with_filter() -> Result<()> {
        let table_scan = test_table()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("id").gt(lit(1)))?
            .project(vec![col("id")])?
            .build()?;

        let expected = "Projection: test.id\
        \n  Filter: test.id > Int32(1)\
        \n    Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n      TableScan: test";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_table_scan_with_subquery() -> Result<()> {
        let table_scan = test_table()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                in_subquery(col("name"), Arc::new(create_table_with_name("sq")?)),
                and(
                    binary_expr(col("id"), Operator::Eq, lit(1_u32)),
                    binary_expr(col("id"), Operator::Lt, lit(30_u32)),
                ),
            ))?
            .project(vec![col("test.name")])?
            .build()?;

        let expected = "Projection: test.name\
        \n  Filter: test.name IN (<subquery>) AND test.id = UInt32(1) AND test.id < UInt32(30)\
        \n    Subquery:\
        \n      TableScan: sq\
        \n    Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n      TableScan: test";

        // after DecorrelatePredicateSubquery, the plan look like below
        // let expected = "Projection: test.name
        // \n  Filter: test.id = UInt32(1) AND test.id < UInt32(30)
        // \n    LeftSemi Join:  Filter: test.name = __correlated_sq_1.id
        // \n      Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)
        // \n        TableScan: test
        // \n      SubqueryAlias: __correlated_sq_1
        // \n        Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)
        // \n          TableScan: sq";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_table_scan_with_join() -> Result<()> {
        let left_table = create_table_with_name("left")?;
        let right_table = create_table_with_name("right")?;
        let plan = LogicalPlanBuilder::from(left_table)
            .join(
                right_table,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("left.id")],
                    vec![Column::from_qualified_name("right.id")],
                ),
                None,
            )?
            .build()?;

        let expected = "Inner Join: left.id = right.id\
        \n  Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n    TableScan: left\
        \n  Filter: _timestamp >= Int64(0) AND _timestamp < Int64(5)\
        \n    TableScan: right";

        assert_optimized_plan_equal(plan, expected)
    }
}
