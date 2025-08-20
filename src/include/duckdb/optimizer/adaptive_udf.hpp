//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/adaptive_udf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {
class Optimizer;

//! The AdaptiveUDF pass rewrites the query plan to adaptively evaluate UDF predicates
class AdaptiveUDF {
public:
	explicit AdaptiveUDF(Optimizer &optimizer, int64_t fixed_placement);

public:
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	unique_ptr<LogicalOperator> RewriteUDFSubPlan(unique_ptr<LogicalOperator> op);

	Optimizer &optimizer;
	int64_t fixed_placement;
};
} // namespace duckdb
