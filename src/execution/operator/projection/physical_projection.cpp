#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include <iostream>
namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions,
	                         const unique_ptr<PhysicalOperator> &filter)
	    : executor(context.client, expressions) {
		if (filter) {
			intermediate_chunk = make_uniq<DataChunk>();
			intermediate_chunk->Initialize(Allocator::DefaultAllocator(), filter->GetTypes());
			filter_state = filter->GetOperatorState(context);
		}
	}

	unique_ptr<DataChunk> intermediate_chunk;
	unique_ptr<OperatorState> filter_state;
	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality, unique_ptr<PhysicalOperator> op)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)), filter(std::move(op)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<ProjectionState>();

	if (filter) {
		filter->Execute(context, input, *(state.intermediate_chunk), gstate, *state.filter_state);

		auto &filter_state = state.filter_state->Cast<FilterState>();
		auto &conjunction_state = filter_state.executor.GetStates()[0]->root_state->Cast<ConjunctionState>();
		auto &adaptive_filter = conjunction_state.adaptive_filter;

		vector<uint32_t> scalar_costs;
		int count = 0;
		for (auto &formula : plan_costs) {
			std::cout << "Parametric formula f[" << count << "](c,s): " << formula.scalar_component << " + "
			          << formula.cost_component << "c + " << formula.selectivity_component << "s" << std::endl;
			auto cost = adaptive_filter->GetSampledCost();
			auto selectivity = adaptive_filter->GetSampledSelectivity();
			std::cout << "Substituing c = " << cost << std::endl;
			std::cout << "Substituing s = " << selectivity << std::endl;
			scalar_costs.push_back(static_cast<uint32_t>(formula.scalar_component + formula.cost_component * cost +
			                                             formula.selectivity_component * selectivity));
			std::cout << "Parametric formula f[" << count << "](c,s): " << scalar_costs.back() << std::endl;
			++count;
		}

		auto it = std::min_element(scalar_costs.begin(), scalar_costs.end());
		auto idx = std::distance(scalar_costs.begin(), it);
		std::cout << "Best plan is: f[" << idx << "] with cost: " << scalar_costs[idx] << std::endl << std::endl;

		state.executor.Execute(*(state.intermediate_chunk), chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list, filter);
}

unique_ptr<PhysicalOperator>
PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
                                         const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
                                         const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality) {

	vector<unique_ptr<Expression>> proj_selects;
	proj_selects.reserve(proj_types.size());

	if (left_projection_map.empty()) {
		for (storage_t i = 0; i < lhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	} else {
		for (auto i : left_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	}
	const auto left_cols = lhs_types.size();

	if (right_projection_map.empty()) {
		for (storage_t i = 0; i < rhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}

	} else {
		for (auto i : right_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}
	}

	return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
}

InsertionOrderPreservingMap<string> PhysicalProjection::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		auto &expr = select_list[i];
		projections += expr->GetName();
	}
	result["__projections__"] = projections;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
