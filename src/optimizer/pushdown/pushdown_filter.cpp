#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();

	if (filter.HasProjectionMap()) {
		return FinishPushdown(std::move(op));
	}
	vector<unique_ptr<Expression>> udf_expressions;
	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		// copy any UDF expressions
		if (expression->ContainsUDF()) {
			udf_expressions.push_back(expression->Copy());
		}
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	GenerateFilters();
	auto child = Rewrite(std::move(filter.children[0]));

	if (udf_expressions.empty()) {
		return child;
	}

	// keep the UDF filter at the top
	auto parent = make_uniq<LogicalFilter>();
	if (child->has_estimated_cardinality) {
		parent->SetEstimatedCardinality(child->estimated_cardinality);
	}
	parent->expressions = std::move(udf_expressions);
	parent->children.push_back(std::move(child));
	return std::move(parent);
}

} // namespace duckdb
