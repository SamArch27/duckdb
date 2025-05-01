//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/binding_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

//! The BindingRewriter updates bindings from old to new
class BindingRewriter : public LogicalOperatorVisitor {
public:
	explicit BindingRewriter(const vector<pair<ColumnBinding, ColumnBinding>> &old_new_bindings);

protected:
	vector<pair<ColumnBinding, ColumnBinding>> old_new_bindings;

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};
} // namespace duckdb
