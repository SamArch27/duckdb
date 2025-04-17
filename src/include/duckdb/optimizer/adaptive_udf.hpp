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
class AdaptiveUDF : public LogicalOperatorVisitor {
public:
	explicit AdaptiveUDF(Optimizer &optimizer);

public:
	void VisitOperator(LogicalOperator &op) override;

private:
	Optimizer &optimizer;
};
} // namespace duckdb
