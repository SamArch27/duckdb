#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> AdaptiveUDF::RewriteUDFSubPlan(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	std::cout << "Rewriting from sub-plan starting from: \n" << std::endl;
	std::cout << op->ToString() << std::endl;
	return op;
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

	// TODO:
	// 1. Find the sub-plan containing the UDF predicate filters (highest and lowest UDF filters)
	// 2. Add a projection node for "best" above the lowest UDF filter with a hardcoded value
	// 3. Pull-up the project up (by adding the column) through all of the nodes in the sub-plan
	// 4. Rewrite each of the UDF filters to the form: ((best != k) OR (best = k AND udf(...)))
	// 5. Compute cost formulas for each plan
	// 6. Make the projection plug in the batch cost/selectivity from the lowest filter when computing the "best"
	// value
}

} // namespace duckdb
