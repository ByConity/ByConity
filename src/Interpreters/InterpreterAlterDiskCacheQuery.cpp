#include <Interpreters/InterpreterAlterDiskCacheQuery.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <CloudServices/CnchPartsHelper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include "Common/tests/gtest_global_context.h"

namespace DB
{
InterpreterAlterDiskCacheQuery::InterpreterAlterDiskCacheQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterAlterDiskCacheQuery::execute()
{
    const auto & query = query_ptr->as<ASTAlterDiskCacheQuery &>();
    /// apply settings
    if (query.settings_ast)
    {
        InterpreterSetQuery(query.settings_ast, getContext()).executeForCurrentContext();
    }

    StoragePtr table = DatabaseCatalog::instance().getTable({query.database, query.table}, getContext());
    auto * storage = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!storage)
        throw Exception("Preload only support CnchMergeTree engine", ErrorCodes::LOGICAL_ERROR);

    if (query.action == ASTAlterDiskCacheQuery::Action::DROP && query.type == ASTAlterDiskCacheQuery::Type::MANIFEST)
    {
        String version = query.version ? query.version->as<ASTLiteral>()->value.get<String>() : "";
        storage->sendDropManifestDiskCacheTasks(getContext(), version, query.sync);
        return {};
    }

    ServerDataPartsVector parts;
    if (query.partition)
    {
        String partition_id = storage->getPartitionIDFromQuery(query.partition, getContext());
        parts = getContext()->getCnchCatalog()->getServerDataPartsInPartitions(table, {partition_id}, getContext()->getTimestamp(), nullptr);
    }
    else
    {
        parts = storage->getAllPartsWithDBM(getContext()).first;
    }
    parts = CnchPartsHelper::calcVisibleParts(parts, false);

    if (query.action == ASTAlterDiskCacheQuery::Action::PRELOAD)
    {
        storage->sendPreloadTasks(getContext(), std::move(parts), query.sync, getContext()->getSettings().parts_preload_level, time(nullptr));
    }
    else if (query.action == ASTAlterDiskCacheQuery::Action::DROP)
    {
        storage->sendDropDiskCacheTasks(getContext(), std::move(parts), query.sync, getContext()->getSettings().drop_vw_disk_cache);
    }
    else
    {
        throw Exception("Unknown alter action for disk cache query", ErrorCodes::NOT_IMPLEMENTED);
    }

    return {};
}
}
