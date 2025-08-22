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
		auto it = parent.find(stream.back());
		if (it == parent.end()) {
			break;
		}
		stream.push_back(it->second);
	}

	auto placement = 0;
	for (idx_t i = 0; i < stream.size(); ++i) {
		auto &op = stream[i];
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op->Cast<LogicalFilter>();
			if (filter.IsUDFFilter()) {
				++placement;

				if (fixed_placement == 0) {
					continue;
				}
				// if the position is hardcoded then clear any other filters
				if (placement != fixed_placement) {
					filter.expressions.clear();
				}
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op->Cast<LogicalComparisonJoin>();
			auto &conds = join.conditions;
			// erase any UDF conditions in non-pipeline breaking joins (coming from the LHS)
			conds.erase(std::remove_if(conds.begin(), conds.end(),
			                           [&](const JoinCondition &cond) { return cond.left->ContainsUDF(); }),
			            conds.end());
			// remove duplicates
			vector<JoinCondition> new_conds;
			for (auto &cond : conds) {
				if (std::any_of(new_conds.begin(), new_conds.end(), [&](const JoinCondition &new_cond) {
					    return new_cond.comparison == cond.comparison && Expression::Equals(new_cond.left, cond.left) &&
					           Expression::Equals(new_cond.right, cond.right);
				    })) {
					continue;
				}
				new_conds.push_back(std::move(cond));
			}
			join.conditions = std::move(new_conds);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator in stream!");
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
