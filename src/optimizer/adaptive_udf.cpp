#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> AdaptiveUDF::Rewrite(unique_ptr<LogicalOperator> op) {

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
