//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/optimizer/adaptive_udf.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PROJECTION;

public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality, unique_ptr<PhysicalOperator> filter = nullptr);

	vector<unique_ptr<Expression>> select_list;
	unique_ptr<PhysicalOperator> filter;
	vector<ParametricCost> plan_costs;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	static unique_ptr<PhysicalOperator>
	CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
	                     const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
	                     const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality);
};

} // namespace duckdb
