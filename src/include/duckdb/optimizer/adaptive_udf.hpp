//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/adaptive_udf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Optimizer;

//! The AdaptiveUDF pass rewrites the query plan to adaptively evaluate UDF predicates
class AdaptiveUDF {
public:
	explicit AdaptiveUDF(Optimizer &optimizer);

public:
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	Optimizer &optimizer;
};
} // namespace duckdb
