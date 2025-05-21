#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector.hpp"
namespace duckdb {

AdaptiveFilter::AdaptiveFilter(const Expression &expr) : observe_interval(10), execute_interval(20), warmup(true) {
	auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
	D_ASSERT(conj_expr.children.size() > 1);
	if (conj_expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		disable_permutations = true;
	}
	bool is_lowest = false;
	for (idx_t idx = 0; idx < conj_expr.children.size(); idx++) {
		permutation.push_back(idx);
		if (conj_expr.children[idx]->CanThrow()) {
			disable_permutations = true;
		}
		if (idx != conj_expr.children.size() - 1) {
			swap_likeliness.push_back(100);
		}
		if (conj_expr.children[idx]->IsLowest()) {
			is_lowest_udf_filter = true;
		}
	}
	right_random_border = 100 * (conj_expr.children.size() - 1);
	tuples_before_filter = 0;
	tuples_after_filter = 0;

	if (is_lowest_udf_filter) {
		// disable redundant AND true
		disable_permutations = false;
		permutation[1] = optional_idx();
	}
}

AdaptiveFilter::AdaptiveFilter(const TableFilterSet &table_filters)
    : observe_interval(10), execute_interval(20), warmup(true) {
	permutation = ExpressionHeuristics::GetInitialOrder(table_filters);
	for (idx_t idx = 1; idx < table_filters.filters.size(); idx++) {
		swap_likeliness.push_back(100);
	}
	right_random_border = 100 * (table_filters.filters.size() - 1);
	tuples_before_filter = 0;
	tuples_after_filter = 0;
}

AdaptiveFilterState AdaptiveFilter::BeginFilter() const {
	if (permutation.size() <= 1 || disable_permutations) {
		return AdaptiveFilterState();
	}
	AdaptiveFilterState state;
	state.start_time = high_resolution_clock::now();
	return state;
}

void AdaptiveFilter::EndFilter(AdaptiveFilterState state) {
	if (permutation.size() <= 1 || disable_permutations) {
		// nothing to permute
		return;
	}
	auto end_time = high_resolution_clock::now();
	tuples_before_filter += state.tuples_before_filter;
	tuples_after_filter += state.tuples_after_filter;
	std::chrono::duration<double, std::nano> diff = end_time - state.start_time;
	AdaptRuntimeStatistics(diff.count());
}

double AdaptiveFilter::GetSampledCost() {
	// nanoseconds for a UDF batch
	// nanoseconds of a predicate batch
	return (tuples_before_filter == 0) ? 0 : (static_cast<double>(runtime_sum) / 5000);
}

double AdaptiveFilter::GetSampledSelectivity() {
	return (tuples_before_filter == 0) ? 0 : static_cast<double>(tuples_after_filter) / tuples_before_filter;
}

void AdaptiveFilter::AdaptRuntimeStatistics(double duration) {
	iteration_count++;
	runtime_sum += duration;

	if (is_lowest_udf_filter) {
		// toggle the filter off after 5 iterations
		if (iteration_count == 5) {
			permutation[0] = optional_idx();
		}
		// reset statistics for the warmup period
		else if (iteration_count < 5) {
			tuples_before_filter = 0;
			tuples_after_filter = 0;
			runtime_sum = 0;
		}
		return;
	}

	D_ASSERT(!disable_permutations);
	if (!warmup) {
		// the last swap was observed
		if (observe && iteration_count == observe_interval) {
			// keep swap if runtime decreased, else reverse swap
			if (prev_mean - (runtime_sum / static_cast<double>(iteration_count)) <= 0) {
				// reverse swap because runtime didn't decrease
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// decrease swap likeliness, but make sure there is always a small likeliness left
				if (swap_likeliness[swap_idx] > 1) {
					swap_likeliness[swap_idx] /= 2;
				}
			} else {
				// keep swap because runtime decreased, reset likeliness
				swap_likeliness[swap_idx] = 100;
			}
			observe = false;

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		} else if (!observe && iteration_count == execute_interval) {
			// save old mean to evaluate swap
			prev_mean = runtime_sum / static_cast<double>(iteration_count);

			// get swap index and swap likeliness
			// a <= i <= b
			auto random_number = generator.NextRandomInteger(1, NumericCast<uint32_t>(right_random_border));

			swap_idx = random_number / 100;                    // index to be swapped
			idx_t likeliness = random_number - 100 * swap_idx; // random number between [0, 100)

			// check if swap is going to happen
			if (swap_likeliness[swap_idx] > likeliness) { // always true for the first swap of an index
				// swap
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// observe whether swap will be applied
				observe = true;
			}

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		}
	} else {
		if (iteration_count == 5) {
			// initially set all values
			iteration_count = 0;
			runtime_sum = 0.0;
			observe = false;
			warmup = false;
		}
	}
}

} // namespace duckdb
