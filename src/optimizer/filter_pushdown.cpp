#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
namespace duckdb {

using Filter = FilterPushdown::Filter;

void FilterPushdown::CheckMarkToSemi(LogicalOperator &op, unordered_set<idx_t> &table_bindings) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::MARK) {
			break;
		}
		// if an operator above the mark join includes the mark join index,
		// then the mark join cannot be converted to a semi join
		if (table_bindings.find(join.mark_index) != table_bindings.end()) {
			join.convert_mark_to_semi = false;
		}
		break;
	}
	// you need to store table.column index.
	// if you get to a projection, you need to change the table_bindings passed so they reflect the
	// table index of the original expression they originated from.
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// when we encounter a projection, replace the table_bindings with
		// the tables in the projection
		auto &proj = op.Cast<LogicalProjection>();
		auto proj_bindings = proj.GetColumnBindings();
		unordered_set<idx_t> new_table_bindings;
		for (auto &binding : proj_bindings) {
			auto col_index = binding.column_index;
			auto &expr = proj.expressions.at(col_index);
			ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
				if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &col_ref = child.Cast<BoundColumnRefExpression>();
					new_table_bindings.insert(col_ref.binding.table_index);
				}
			});
			table_bindings = new_table_bindings;
		}
		break;
	}
	// It's possible a mark join index makes its way into a group by as the grouping index
	// when that happens we need to keep track of it to make sure we do not convert a mark join to semi.
	// see filter_pushdown_into_subquery.
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		auto aggr_bindings = aggr.GetColumnBindings();
		vector<ColumnBinding> bindings_to_keep;
		for (auto &expr : aggr.groups) {
			ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
				if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &col_ref = child.Cast<BoundColumnRefExpression>();
					bindings_to_keep.push_back(col_ref.binding);
				}
			});
		}
		for (auto &expr : aggr.expressions) {
			ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
				if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &col_ref = child.Cast<BoundColumnRefExpression>();
					bindings_to_keep.push_back(col_ref.binding);
				}
			});
		}
		table_bindings = unordered_set<idx_t>();
		for (auto &expr_binding : bindings_to_keep) {
			table_bindings.insert(expr_binding.table_index);
		}
		break;
	}
	default:
		break;
	}

	// recurse into the children to find mark joins and project their indexes.
	for (auto &child : op.children) {
		CheckMarkToSemi(*child, table_bindings);
	}
}

FilterPushdown::FilterPushdown(Optimizer &optimizer, bool udf_filter_pushdown, bool convert_mark_joins)
    : optimizer(optimizer), combiner(optimizer.context), udf_filter_pushdown(udf_filter_pushdown),
      convert_mark_joins(convert_mark_joins) {
}

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	unique_ptr<LogicalOperator> result;
	vector<unique_ptr<Expression>> udf_expressions;

	if (udf_filter_pushdown) {
		for (auto &filter : filters) {
			if (filter) {
				if (auto &expr = filter->filter) {
					if (expr->ContainsUDF()) {
						if (std::any_of(
						        udf_expressions.begin(), udf_expressions.end(),
						        [&](unique_ptr<Expression> &udf_expr) { return Expression::Equals(udf_expr, expr); })) {
							continue;
						}
						udf_expressions.push_back(expr->Copy());
					}
				}
			}
		}
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		result = PushdownAggregate(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_FILTER: {
		// if the filter contains only udf predicates we are pushing down already, don't add the filter again
		if (udf_filter_pushdown) {
			auto &exprs = op->Cast<LogicalFilter>().expressions;
			if (std::all_of(exprs.begin(), exprs.end(), [&](unique_ptr<Expression> &expr) {
				    for (auto &udf_expr : udf_expressions) {
					    if (Expression::Equals(udf_expr, expr)) {
						    return true;
					    }
				    }
				    return false;
			    })) {
				return PushdownFilter(std::move(op));
			}
		}
		result = PushdownFilter(std::move(op));
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		result = PushdownCrossProduct(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		result = PushdownJoin(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		result = PushdownProjection(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		result = PushdownSetOperation(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		result = PushdownDistinct(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		result = std::move(op);
		break;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// we can't push filters into the materialized CTE (LHS), but we do want to recurse into it
		FilterPushdown pushdown(optimizer, udf_filter_pushdown, convert_mark_joins);
		op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
		// we can push filters into the rest of the query plan (RHS)
		op->children[1] = Rewrite(std::move(op->children[1]));
		result = std::move(op);
		break;
	}
	case LogicalOperatorType::LOGICAL_GET:
		result = PushdownGet(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		result = PushdownLimit(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result = PushdownWindow(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		result = PushdownUnnest(std::move(op));
		break;
	default:
		result = FinishPushdown(std::move(op));
		break;
	}

	if (udf_filter_pushdown) {
		// attach UDF filters above
		for (auto &expr : udf_expressions) {
			auto dup_filter = make_uniq<LogicalFilter>();
			if (result->has_estimated_cardinality) {
				dup_filter->SetEstimatedCardinality(result->estimated_cardinality);
			}
			dup_filter->expressions.push_back(expr->Copy());
			dup_filter->children.push_back(std::move(result));
			result = std::move(dup_filter);
		}
	}
	return result;
}

ClientContext &FilterPushdown::GetContext() {
	return optimizer.GetContext();
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	if (join.HasProjectionMap()) {
		// cannot push down further otherwise the projection maps won't be preserved
		return FinishPushdown(std::move(op));
	}

	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		//	AsOf joins can't push anything into the RHS, so treat it as a left join
		if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
		}
		return PushdownInnerJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SEMI:
	case JoinType::ANTI:
		return PushdownSemiAntiJoin(std::move(op));
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(std::move(op));
	}
}
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(std::move(f->filter));
		D_ASSERT(result != FilterResult::UNSUPPORTED);
		(void)result;
	}
	filters.clear();
}

FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &child_expr : expressions) {
		if (combiner.AddFilter(std::move(child_expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (!filters.empty()) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_uniq<Filter>();
		f->filter = std::move(filter);
		f->ExtractBindings();
		filters.push_back(std::move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::AddLogicalFilter(unique_ptr<LogicalOperator> op,
                                                             vector<unique_ptr<Expression>> expressions) {
	if (expressions.empty()) {
		// No left expressions, so needn't to add an extra filter operator.
		return op;
	}
	auto filter = make_uniq<LogicalFilter>();
	if (op->has_estimated_cardinality) {
		// set the filter's estimated cardinality as the child op's.
		// if the filter is created during the filter pushdown optimization, the estimated cardinality will be later
		// overridden during the join order optimization to a more accurate one.
		// if the filter is created during the statistics propagation, the estimated cardinality won't be set unless
		// set here. assuming the filters introduced during the statistics propagation have little effect in
		// reducing the cardinality, we adopt the the cardinality of the child. this could be improved by MinMax
		// info from the statistics propagation
		filter->SetEstimatedCardinality(op->estimated_cardinality);
	}
	filter->expressions = std::move(expressions);
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPushdown::PushFinalFilters(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<Expression>> expressions;
	for (auto &f : filters) {
		if (std::any_of(expressions.begin(), expressions.end(),
		                [&](unique_ptr<Expression> &expr) { return Expression::Equals(expr, f->filter); })) {
			continue;
		}
		expressions.push_back(std::move(f->filter));
	}

	auto filter_op = AddLogicalFilter(std::move(op), std::move(expressions));
	if (filter_op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = filter_op->Cast<LogicalFilter>();
		if (filter.IsUDFFilter()) {
			bool udf_filter_below = HasUDFFilterInSubtree(filter_op->children[0].get());
			if (!udf_filter_below) {
				for (auto &expr : filter.expressions) {
					expr->SetLowest();
				}
			}
		}
	}
	return filter_op;
}

bool FilterPushdown::HasUDFFilterInSubtree(LogicalOperator *op) {
	if (!op) {
		return false;
	}

	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		if (filter.IsUDFFilter()) {
			return true;
		}
	}

	for (auto &child : op->children) {
		if (HasUDFFilterInSubtree(child.get())) {
			return true;
		}
	}

	return false;
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer, udf_filter_pushdown, convert_mark_joins);
		child = pushdown.Rewrite(std::move(child));
	}
	// now push any existing filters
	return PushFinalFilters(std::move(op));
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb
