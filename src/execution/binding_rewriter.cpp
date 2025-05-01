#include "duckdb/execution/binding_rewriter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <iostream>

namespace duckdb {

BindingRewriter::BindingRewriter(const vector<pair<ColumnBinding, ColumnBinding>> &old_new_bindings)
    : old_new_bindings(old_new_bindings) {
}

unique_ptr<Expression> BindingRewriter::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {

	for (auto &[old_b, new_b] : old_new_bindings) {
		if (old_b == expr.binding) {
			std::cout << "Rewrote binding in BindingRewriter!!" << std::endl;
			expr.binding = new_b;
			break;
		}
	}

	return std::move(*expr_ptr);
}

} // namespace duckdb
