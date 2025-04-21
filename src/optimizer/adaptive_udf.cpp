#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/binder.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> AdaptiveUDF::RewriteUDFSubPlan(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);

	unordered_map<LogicalOperator *, LogicalOperator *> parent;
	queue<LogicalOperator *> q;
	LogicalOperator *match = nullptr;
	q.push(op.get());
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

	vector<unique_ptr<Expression>> project_expr;
	project_expr.emplace_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	auto project = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(project_expr));

	std::cout << "Project: \n" << std::endl;
	std::cout << project->ToString() << std::endl;

	// TODO:
	// 1. Add a projection node for "best" above the lowest UDF filter with a hardcoded value
	// 2. Pull-up the project up (by adding the column) through all of the nodes in the sub-plan
	// 3. Rewrite each of the UDF filters to the form: ((best != k) OR (best = k AND udf(...)))

	// Next TODO:
	// 4. Compute cost formulas for each plan

	// After Kyle's part TODO:
	// 5. Make the projection plug in the batch cost/selectivity from the lowest filter when computing the "best"
	// value

	return op;
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
