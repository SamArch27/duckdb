#include "duckdb/optimizer/adaptive_udf.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include <iostream>

namespace duckdb {

AdaptiveUDF::AdaptiveUDF(Optimizer &optimizer) : optimizer(optimizer) {
}

void AdaptiveUDF::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		std::cout << "Matched on filter!\n" << std::endl;
		std::cout << op.ToString() << std::endl;
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

} // namespace duckdb
