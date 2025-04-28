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

	vector<unique_ptr<Expression>> udf_filters;
	vector<unique_ptr<Expression>> non_udf_filters;

	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		if (expression->ContainsUDF()) {
			udf_filters.push_back(expression->Copy());
		}

		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}

	// continue as normal if udf filters are being pushed down
	if (udf_filter_pushdown) {
		GenerateFilters();

		return Rewrite(std::move(filter.children[0]));
	}

	// otherwise split filters into UDFs and non-UDFs and push down non-UDFs
	filter.expressions.clear();
	vector<unique_ptr<Expression>> udfs, non_udfs;

	combiner.GenerateFilters([&](unique_ptr<Expression> expr) {
		if (expr->ContainsUDF()) {
			udfs.push_back(std::move(expr));
		} else {
			non_udfs.push_back(std::move(expr));
		}
	});

	// Add non UDF-filters back in to the combiner a second time
	for (auto &expression : non_udfs) {
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}

	// Generate filters
	GenerateFilters();

	// Rewrite the child
	auto new_child = Rewrite(std::move(filter.children[0]));

	// For each UDF create a new Filter and add it above the current one
	unique_ptr<LogicalOperator> current_op = std::move(new_child);
	for (auto &expr : udfs) {
		auto udf_filter = make_uniq<LogicalFilter>();
		udf_filter->expressions.push_back(std::move(expr));
		udf_filter->children.push_back(std::move(current_op));
		current_op = std::move(udf_filter);
	}

	return std::move(current_op);
}

} // namespace duckdb

