#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>

namespace DB
{
class MaterializedViewMemoryCache
{
public:
    static MaterializedViewMemoryCache & instance();

    std::optional<MaterializedViewStructurePtr> getMaterializedViewStructure(
        const StorageID & database_and_table_name, ContextMutablePtr context);

private:
    MaterializedViewMemoryCache() = default;

    // todo: implement lru cache.
    // std::unordered_map<String, MaterializedViewStructurePtr> materialized_view_structures;
    // mutable std::shared_mutex mutex;
};
}
