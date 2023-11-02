#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
// Push filter into storage, returns a remaining filter which consists of criteria that can not be evaluated completely in storage.
// For example, criteria on partition keys shoule not be returned.
ASTPtr pushFilterIntoStorage(ASTPtr query_filter, const MergeTreeData * merge_tree_data, SelectQueryInfo & query_info, ContextPtr context);
}
