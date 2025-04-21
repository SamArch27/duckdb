#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> AdaptiveUDF::Rewrite(unique_ptr<LogicalOperator> op) {

	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		auto &exprs = filter.expressions;
		if (std::all_of(exprs.begin(), exprs.end(),
		                [&](unique_ptr<Expression> &expr) { return expr->ContainsUDF(); })) {
			std::cout << "UDF Filter: \n" << std::endl;
			std::cout << op->ToString() << std::endl;
		}
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		op->children[i] = Rewrite(std::move(op->children[i]));
	}

	// TODO:
	// 1. Find the sub-plan containing the UDF predicate filters (highest and lowest UDF filters)
	// 2. Add a projection node for "best" above the lowest UDF filter with a hardcoded value
	// 3. Pull-up the project up (by adding the column) through all of the nodes in the sub-plan
	// 4. Rewrite each of the UDF filters to the form: ((best != k) OR (best = k AND udf(...)))
	// 5. Compute cost formulas for each plan
	// 6. Make the projection plug in the batch cost/selectivity from the lowest filter when computing the "best" value

	return op;
}

} // namespace duckdb
