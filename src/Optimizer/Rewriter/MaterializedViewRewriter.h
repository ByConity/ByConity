#pragma once

#include <Optimizer/Equivalences.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTTableColumnReference.h>
#include <QueryPlan/Assignment.h>

namespace DB
{
/**
  * MaterializedViewRewriter is based on "Optimizing Queries Using Materialized Views:
  * A Practical, Scalable Solution" by Goldstein and Larson.
  */
class MaterializedViewRewriter : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "MaterializedViewRewriter"; }

private:
    static std::map<String, std::vector<MaterializedViewStructurePtr>>
    getRelatedMaterializedViews(QueryPlan & plan, ContextMutablePtr context);
};
}
