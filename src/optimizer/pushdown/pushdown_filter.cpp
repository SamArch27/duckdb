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

	// add UDF filters to the FilterCombiner, skip everything else
	if (udf_filter_pushdown) {
		for (auto &expression : filter.expressions) {
			// skip non-UDF filters
			if (!expression->ContainsUDF()) {
				continue;
			}
			// add the UDF filters for pushdown
			if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
		}
		// push down those UDF filters
		GenerateFilters();
		return Rewrite(std::move(filter.children[0]));
	}

	// otherwise add the non-UDF filters to the FilterCombiner
	vector<unique_ptr<Expression>> udf_filters;
	for (auto &expression : filter.expressions) {
		// skip non-UDF filters
		if (expression->ContainsUDF()) {
			udf_filters.push_back(expression->Copy());
			continue;
		}
		// add the non-UDF filters for pushdown
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}

	// push down the non-UDF filters
	GenerateFilters();

	// clear the current filter node
	filter.expressions.clear();

	// rewrite the child
	auto new_child = Rewrite(std::move(filter.children[0]));

	// for each UDF create a new filter node and add it above the current one
	unique_ptr<LogicalOperator> current_op = std::move(new_child);
	for (auto &expr : udf_filters) {
		auto udf_filter = make_uniq<LogicalFilter>();
		udf_filter->expressions.push_back(std::move(expr));
		udf_filter->children.push_back(std::move(current_op));
		current_op = std::move(udf_filter);
	}
	return std::move(current_op);
}

} // namespace duckdb
