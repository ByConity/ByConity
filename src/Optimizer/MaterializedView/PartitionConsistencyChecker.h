#pragma once

#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <utility>
#include <vector>

namespace DB
{
struct PartitionCheckResult
{
    StoragePtr depend_storage;
    size_t unique_id = 0;
    ASTPtr union_query_partition_predicate;
    ASTPtr mview_partition_predicate;
};

std::optional<PartitionCheckResult>
checkMaterializedViewPartitionConsistency(MaterializedViewStructurePtr structure, ContextMutablePtr context);
}
