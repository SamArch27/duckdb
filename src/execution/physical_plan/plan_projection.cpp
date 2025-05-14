#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	D_ASSERT(op.children.size() == 1);

	bool udf_filter = false;
	if (op.children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.children[0]->Cast<LogicalFilter>();
		if (filter.expressions.size() == 2) {
			if (filter.expressions[0]->ContainsUDF()) {
				udf_filter = true;
			}
		}
	}

	auto plan = CreatePlan(*op.children[0]);

#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(!expr->IsWindow());
		D_ASSERT(!expr->IsAggregate());
	}
#endif
	if (plan->types.size() == op.types.size()) {
		// check if this projection can be omitted entirely
		// this happens if a projection simply emits the columns in the same order
		// e.g. PROJECTION(#0, #1, #2, #3, ...)
		bool omit_projection = true;
		for (idx_t i = 0; i < op.types.size(); i++) {
			if (op.expressions[i]->GetExpressionType() == ExpressionType::BOUND_REF) {
				auto &bound_ref = op.expressions[i]->Cast<BoundReferenceExpression>();
				if (bound_ref.index == i) {
					continue;
				}
			}
			omit_projection = false;
			break;
		}
		if (omit_projection) {
			// the projection only directly projects the child' columns: omit it entirely
			return plan;
		}
	}

	if (udf_filter) {
		auto &filter = *plan;
		auto below_filter = std::move(plan->children[0]);
		plan->children.clear();
		auto projection = make_uniq<PhysicalProjection>(op.types, std::move(op.expressions), op.estimated_cardinality,
		                                                std::move(plan));
		projection->children.push_back(std::move(below_filter));
		return std::move(projection);
	}

	auto projection = make_uniq<PhysicalProjection>(op.types, std::move(op.expressions), op.estimated_cardinality);
	projection->children.push_back(std::move(plan));
	return std::move(projection);
}

} // namespace duckdb
