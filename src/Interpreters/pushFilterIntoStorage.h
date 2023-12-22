#pragma once

#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/SelectQueryInfo.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>

namespace DB
{
// Push filter into storage, returns a remaining filter which consists of criteria that can not be evaluated completely in storage.
// For example, criteria on partition keys shoule not be returned.
ASTPtr pushFilterIntoStorage(ASTPtr query_filter, const MergeTreeMetaBase * merge_tree_data, SelectQueryInfo & query_info, PlanNodeStatisticsPtr storage_statistics, const NamesAndTypes & names_and_types, ContextMutablePtr context);
}
