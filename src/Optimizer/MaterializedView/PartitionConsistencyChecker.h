#pragma once

#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/StorageMaterializedView.h>

#include <memory>
#include <utility>
#include <vector>

namespace DB
{
std::optional<ASTPtr> getQueryRelatedPartitions(
    const std::vector<PlanNodePtr> & query_table_scans,
    const ConstASTs & query_predicates,
    const std::unordered_map<PlanNodePtr, String> & partition_keys,
    const SymbolTransformMap & query_transform_map,
    ContextPtr context,
    Poco::Logger * logger);

std::optional<ASTPtr> getViewRefreshedPartitions(const StoragePtr & view, const String & partition_key, ContextPtr context);

struct PartitionCheckResult
{
    StoragePtr depend_storage;
    size_t unique_id = 0;
    ASTPtr union_query_partition_predicate;
    ASTPtr mview_partition_predicate;
};

std::optional<PartitionCheckResult> checkMaterializedViewPartitionConsistency(StorageMaterializedView * mview, ContextMutablePtr context);
}
