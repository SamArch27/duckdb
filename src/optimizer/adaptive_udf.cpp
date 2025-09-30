#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/config.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer, int64_t fixed_placement)
    : optimizer(optimizer), fixed_placement(fixed_placement) {
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
								child_filter.expressions.clear();
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
		auto prev = stream.back();
		auto it = parent.find(prev);
		if (it == parent.end()) {
			break;
		}
		auto curr = it->second;
		stream.push_back(curr);
	}
	std::reverse(stream.begin(), stream.end());

	vector<LogicalOperator *> new_stream;
	new_stream.push_back(stream.front());
	for (idx_t i = 1; i < stream.size(); ++i) {
		auto *prev = stream[i - 1];
		auto *curr = stream[i];

		// need to stick a UDF filter between them
		if (prev->type != LogicalOperatorType::LOGICAL_FILTER && curr->type != LogicalOperatorType::LOGICAL_FILTER) {
			// check which child it is
			auto index = (prev->children[0].get() == curr) ? 0 : 1;
			// save the old child
			auto child = std::move(prev->children[index]);
			// clone the UDF filter and assign it as the new child
			prev->children[index] = match->Copy(optimizer.GetContext());
			// set the UDF filter's child to be the old child
			prev->children[index]->children[0] = std::move(child);
			// add it to the new stream
			new_stream.push_back(prev->children[index].get());
		}

		// unconditionally add the current
		new_stream.push_back(curr);
	}

	// lastly clear the duplicate UDF filter at the bottom
	match->children[0]->expressions.clear();

	// reverse the stream so it's bottom to top again
	std::reverse(new_stream.begin(), new_stream.end());

	auto placement = 0;
	for (idx_t i = 0; i < new_stream.size(); ++i) {
		auto &op = new_stream[i];
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = op->Cast<LogicalFilter>();
			if (filter.IsUDFFilter()) {
				++placement;

				// if fixed_placement is not specified then keep all filters
				if (fixed_placement == 0) {
					continue;
				}
				// if the position is hardcoded then clear any other filters
				if (placement != fixed_placement) {
					filter.expressions.clear();
				}
			}
		}
	}

	return root_filter;
}

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
