#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/binding_rewriter.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer, int64_t best)
    : optimizer(optimizer), best(best), rewriter(old_new_bindings) {
}

unique_ptr<LogicalOperator> AdaptiveUDF::RewriteUDFSubPlan(unique_ptr<LogicalOperator> root_filter) {
	D_ASSERT(root_filter->type == LogicalOperatorType::LOGICAL_FILTER);

	unordered_map<LogicalOperator *, LogicalOperator *> parent;
	queue<LogicalOperator *> q;
	LogicalOperator *match = nullptr;
	q.push(root_filter.get());
	while (!q.empty()) {
		auto *curr = q.front();
		q.pop();

		// If we have two identical UDF filters then it's a match!
		if (curr->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = curr->Cast<LogicalFilter>();
			if (filter.IsUDFFilter()) {
				D_ASSERT(filter.expressions.size() == 1);
				if (!filter.children.empty()) {
					auto &child = filter.children[0];
					if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
						auto &child_filter = child->Cast<LogicalFilter>();
						if (child_filter.IsUDFFilter()) {
							D_ASSERT(child_filter.expressions.size() == 1);
							if (Expression::Equals(filter.expressions[0], child_filter.expressions[0])) {
								match = curr;
								if (best != 42) {
									child_filter.expressions.clear();
								} else {
									child_filter.expressions.emplace_back(
									    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
								}
								break;
							}
						}
					}
				}
			}
		}

		for (auto &child : curr->children) {
			parent[child.get()] = curr;
			q.push(child.get());
		}
	}

	D_ASSERT(match != nullptr);

	// backtrack the parent to produce the stream
	vector<LogicalOperator *> stream;
	stream.push_back(match);
	while (true) {
		auto it = parent.find(stream.back());
		if (it == parent.end()) {
			break;
		}
		stream.push_back(it->second);
	}

	// reverse the stream to get the right order
	std::reverse(stream.begin(), stream.end());

	for (auto *op : stream) {
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			break;
		default:
			throw NotImplementedException("Unsupported operator in stream!");
		}
	}

	// stick a project between this filter and the lowest UDF filter
	auto &last_filter = stream.back()->Cast<LogicalFilter>();

	// re-project the expressions below
	auto &node = last_filter.children[0];
	auto bindings = node->GetColumnBindings();
	node->ResolveOperatorTypes();
	auto types = node->types;
	vector<unique_ptr<Expression>> project_expressions;
	project_expressions.reserve(bindings.size() + 1);
	D_ASSERT(bindings.size() == types.size());
	idx_t new_tbl_idx = optimizer.binder.GenerateTableIndex();
	idx_t tbl_idx = bindings[0].table_index;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		D_ASSERT(tbl_idx == bindings[col_idx].table_index);
		project_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(types[col_idx], bindings[col_idx]));
		old_new_bindings.emplace_back(make_pair(bindings[col_idx], ColumnBinding(new_tbl_idx, col_idx)));
	}
	// project a new column for "best"
	project_expressions.emplace_back(make_uniq<BoundConstantExpression>(Value::INTEGER(best)));
	// make the projection node
	auto project = make_uniq<LogicalProjection>(new_tbl_idx, std::move(project_expressions));
	// propagate the cardinality of the node below
	if (last_filter.children[0]->has_estimated_cardinality) {
		project->SetEstimatedCardinality(last_filter.children[0]->estimated_cardinality);
	}

	// detach the filter
	auto detached_filter = std::move(last_filter.children[0]);

	// remove the child
	last_filter.children.clear();

	// rewrite the old bindings above to use the new binding
	for (auto &op : stream) {
		// rewrite the expressions
		rewriter.VisitOperatorExpressions(*op);
	}

	// reattach the node below as a child of the project
	project->AddChild(std::move(detached_filter));

	// attach the project as a child of the parent
	last_filter.AddChild(std::move(project));

	// grab the binding for the "best" column
	auto &new_project = last_filter.children[0];
	auto project_bindings = new_project->GetColumnBindings();
	new_project->ResolveOperatorTypes();
	auto project_types = new_project->types;
	auto project_binding = project_bindings.back();
	auto project_reference = make_uniq<BoundColumnRefExpression>("best", project_types.back(), project_bindings.back());

	// consider filters lowest to highest
	std::reverse(stream.begin(), stream.end());

	// rewrite any UDF filter in the stream to be of the form (best != k OR udf(...))
	auto placement = 0;
	for (auto &op : stream) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = op->Cast<LogicalFilter>();
			if (filter.IsUDFFilter()) {
				// rewrite udf(...) to (best != k OR udf(...))
				++placement;
				// create best != k
				auto comparison =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, project_reference->Copy(),
				                                         make_uniq<BoundConstantExpression>(Value::INTEGER(placement)));
				// disjunct it with the original UDF expression
				auto disjunction = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_OR, std::move(comparison), std::move(filter.expressions[0]));
				filter.expressions[0] = std::move(disjunction);
			}
		}
	}

	// Now we need to update the bindings for each node in the stream
	for (int i = 0; i < stream.size(); ++i) {
		auto &op = stream[i];

		if (!op->HasProjectionMap()) {
			continue;
		}

		switch (op->type) {
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
			auto &join = op->Cast<LogicalJoin>();

			auto left_bindings = join.children[0]->GetColumnBindings();
			auto right_bindings = join.children[1]->GetColumnBindings();
			auto left_it = std::find(left_bindings.begin(), left_bindings.end(), project_binding);
			auto right_it = std::find(right_bindings.begin(), right_bindings.end(), project_binding);
			if (left_it == left_bindings.end() && right_it == right_bindings.end()) {
				throw NotImplementedException("Should have binding for join but don't!");
			}
			// binding is from the LHS
			if (left_it != left_bindings.end()) {

				// it will inherit the binding automatically, continue
				auto &map = join.left_projection_map;
				if (map.empty()) {
					continue;
				}

				// otherwise insert it into the projection map
				auto offset = std::distance(left_bindings.begin(), left_it);
				for (auto &b : join.left_projection_map) {
					if (b >= offset) {
						++b;
					}
				}

				// add the projection for the next operator
				if (i != stream.size() - 1) {
					map.push_back(offset);
				}
			}
			// binding is from the RHS
			else {

				// it will inherit the binding automatically, continue
				auto &map = join.right_projection_map;
				if (map.empty()) {
					continue;
				}

				// otherwise insert it into the projection map
				auto offset = std::distance(right_bindings.begin(), right_it);
				for (auto &b : join.right_projection_map) {
					if (b >= offset) {
						++b;
					}
				}

				// add the projection for the next operator
				if (i != stream.size() - 1) {
					map.push_back(offset);
				}
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op->Cast<LogicalFilter>();

			// it will inherit the binding automatically, continue
			auto &map = filter.projection_map;
			if (map.empty()) {
				continue;
			}

			auto child_bindings = filter.children[0]->GetColumnBindings();
			auto it = std::find(child_bindings.begin(), child_bindings.end(), project_binding);
			if (it == child_bindings.end()) {
				throw NotImplementedException("Should have binding for child of filter but don't!");
			}

			// otherwise insert it into the projection map
			auto offset = std::distance(child_bindings.begin(), it);
			for (auto &b : filter.projection_map) {
				if (b >= offset) {
					++b;
				}
			}

			// add the projection for the next operator
			if (i != stream.size() - 1) {
				map.push_back(offset);
			}

			break;
		}
		default:
			throw NotImplementedException("Should have binding but don't for %s", EnumUtil::ToString(op->type));
		}
	}

	// TODO: Compute cost formulas for each placement
	struct ParametricCost {
		int scalar_component;
		int cost_component;
		int selectivity_component;
	};

	// costs are initially zero for each placement
	vector<ParametricCost> placement_costs(placement, {0, 0, 0});

	optional_idx distinct_count;
	auto &lowest_filter = new_project->children[0];
	if (lowest_filter->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = lowest_filter->Cast<LogicalFilter>();
		// TODO: Use distinct count once we have implemented caching
		// distinct_count = filter.GetDistinctValues();
	}
	int udf_filter_count = 0;

	for (auto &op : stream) {
		// check if it's a UDF filter
		bool is_udf_filter = false;
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = op->Cast<LogicalFilter>();
			if (filter.IsUDFFilter()) {
				is_udf_filter = true;
			}
		}

		for (int i = 0; i < placement; ++i) {
			// haven't evaluated the UDF filter yet
			if (i >= udf_filter_count) {
				placement_costs[i].scalar_component += op->estimated_cardinality;
			}
			// evaluating the UDF now
			if (i == udf_filter_count && is_udf_filter) {
				if (distinct_count.IsValid()) {
					placement_costs[i].cost_component += MinValue(op->estimated_cardinality, distinct_count.GetIndex());
				} else {
					placement_costs[i].cost_component += op->estimated_cardinality;
				}
			}
			// evaluated the UDF filter already
			if (i < udf_filter_count) {
				placement_costs[i].selectivity_component += op->estimated_cardinality;
			}
		}

		if (is_udf_filter) {
			++udf_filter_count;
		}
	}

	// TODO: Plug them into the projection with Kyle's part

	return root_filter;
} // namespace duckdb

unique_ptr<LogicalOperator> AdaptiveUDF::Rewrite(unique_ptr<LogicalOperator> op) {

	// Match on the top-most UDF filter and rewrite it to be adaptive
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		if (filter.IsUDFFilter()) {
			return RewriteUDFSubPlan(std::move(op));
		}
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		op->children[i] = Rewrite(std::move(op->children[i]));
		rewriter.VisitOperatorExpressions(*op->children[i]);
	}

	return op;
}

} // namespace duckdb
