#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"

#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
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
			std::cout << "Error: Unsupported construct for UDF filter pushing " << std::endl;
			std::cout << "Operating is: \n" << std::endl;
			std::cout << op->ToString() << std::endl;
			D_ASSERT(false);
		}
	}

	// stick a project between this filter and the lowest UDF filter
	auto *last_filter = stream.back();

	// re-project the expressions below
	auto &node = last_filter->children[0];
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
	}
	// project a new column for "best"
	project_expressions.emplace_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	// make the projection node
	auto project = make_uniq<LogicalProjection>(new_tbl_idx, std::move(project_expressions));
	// propagate the cardinality of the node below
	if (last_filter->children[0]->has_estimated_cardinality) {
		project->SetEstimatedCardinality(last_filter->children[0]->estimated_cardinality);
	}

	std::cout << "ADDED PROJECTION!" << std::endl;
	// resolve the column bindings
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*root_filter);

	// attach the node below as a child of the project
	project->AddChild(std::move(last_filter->children[0]));

	// attach the project as a child of the parent
	last_filter->children[0] = std::move(project);

	// // resolve the columns again
	// std::cout << "REATTACHED AND REPRINTING!" << std::endl;
	// resolver.VisitOperator(*root_filter);

	// grab the binding for the "best" column
	auto &new_project = last_filter->children[0];
	auto project_bindings = new_project->GetColumnBindings();
	new_project->ResolveOperatorTypes();
	auto project_types = new_project->types;
	auto project_binding = project_bindings.back();
	auto project_reference = make_uniq<BoundColumnRefExpression>("best", project_types.back(), project_bindings.back());

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
		std::cout << "Op is: " << std::endl;
		std::cout << op->ToString() << std::endl;

		auto bindings = op->GetColumnBindings();
		std::cout << "Bindings are: " << std::endl;
		for (auto &b : op->GetColumnBindings()) {
			std::cout << b.ToString() << std::endl;
		}
		auto it = std::find(bindings.begin(), bindings.end(), project_binding);

		// if the binding for the "best" is found then we're done
		if (it != bindings.end()) {
			std::cout << "Found project in bindings! Continuing!" << std::endl;
			continue;
		}

		switch (op->type) {
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
			std::cout << "Join!" << std::endl;
			auto &join = op->Cast<LogicalJoin>();

			std::cout << "Left projection map before: " << std::endl;
			for (auto &idx : join.left_projection_map) {
				std::cout << idx << std::endl;
			}
			std::cout << "Right projection map before: " << std::endl;
			for (auto &idx : join.right_projection_map) {
				std::cout << idx << std::endl;
			}

			auto left_bindings = join.children[0]->GetColumnBindings();
			auto right_bindings = join.children[1]->GetColumnBindings();
			auto left_it = std::find(left_bindings.begin(), left_bindings.end(), project_binding);
			auto right_it = std::find(right_bindings.begin(), right_bindings.end(), project_binding);
			if (left_it == left_bindings.end() && right_it == right_bindings.end()) {
				std::cout << "Didn't find binding in either child!" << std::endl;
				throw NotImplementedException("Should have binding for join but don't!");
			}
			// binding is from the LHS
			if (left_it != left_bindings.end()) {
				auto offset = std::distance(left_bindings.begin(), left_it);
				std::cout << "Adding offset: " << offset << " to left projection map!" << std::endl;
				std::cout << "Binding at offset is: " << left_bindings[offset].ToString() << std::endl;
				for (auto &b : join.left_projection_map) {
					if (b >= offset) {
						++b;
					}
				}
				join.left_projection_map.push_back(offset);
			}
			// binding is from the RHS
			else {
				auto offset = std::distance(right_bindings.begin(), right_it);
				std::cout << "Adding offset: " << offset << " to right projection map!" << std::endl;
				std::cout << "Binding at offset is: " << right_bindings[offset].ToString() << std::endl;
				for (auto &b : join.right_projection_map) {
					if (b >= offset) {
						++b;
					}
				}
				join.right_projection_map.push_back(offset);
			}

			std::cout << "Left projection map after: " << std::endl;
			for (auto &idx : join.left_projection_map) {
				std::cout << idx << std::endl;
			}
			std::cout << "Right projection map after: " << std::endl;
			for (auto &idx : join.right_projection_map) {
				std::cout << idx << std::endl;
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			std::cout << "Filter!" << std::endl;
			auto &filter = op->Cast<LogicalFilter>();
			if (!filter.projection_map.empty()) {
				std::cout << "Projection map before: " << std::endl;
				for (auto &idx : filter.projection_map) {
					std::cout << idx << std::endl;
				}

				auto child_bindings = filter.children[0]->GetColumnBindings();
				auto it = std::find(child_bindings.begin(), child_bindings.end(), project_binding);
				if (it == child_bindings.end()) {
					std::cout << "Didn't find binding in filter child!" << std::endl;
					throw NotImplementedException("Should have binding for child of filter but don't!");
				}
				auto offset = std::distance(child_bindings.begin(), it);
				std::cout << "Adding offset: " << offset << " to projection map!" << std::endl;
				std::cout << "Binding at offset is: " << child_bindings[offset].ToString() << std::endl;
				for (auto &b : filter.projection_map) {
					if (b >= offset) {
						++b;
					}
				}
				filter.projection_map.push_back(offset);

				std::cout << "Projection map after: " << std::endl;
				for (auto &idx : filter.projection_map) {
					std::cout << idx << std::endl;
				}
			}
			break;
		}
		default:
			throw NotImplementedException("Should have binding but don't for %s", EnumUtil::ToString(op->type));
		}
		std::cout << "PRINTING UPDATED BINDINGS" << std::endl;
		for (auto &b : op->GetColumnBindings()) {
			std::cout << b.ToString() << std::endl;
		}
	}

	// // resolve the columns again
	// resolver.VisitOperator(*root_filter);

	// TODO:
	// 1. Rewrite each of the UDF filters to the form: (best != k OR udf(...))

	// Next TODO:
	// 2. Compute cost formulas for each plan

	// After Kyle's part TODO:
	// 3. Make the projection plug in the batch cost/selectivity from the lowest filter when computing the
	// "best" value

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
	}

	return op;
}

} // namespace duckdb
