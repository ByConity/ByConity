#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <common/logger_useful.h>

namespace DB
{
class MergeTreeMetaBase;

enum class ServerSelectPartsDecision
{
    SELECTED = 0,
    CANNOT_SELECT = 1,
    NOTHING_TO_MERGE = 2,
};

using ServerCanMergeCallback = std::function<bool(const ServerDataPartPtr &, const ServerDataPartPtr &)>;

ServerSelectPartsDecision selectPartsToMerge(
    const MergeTreeMetaBase & data,
    std::vector<ServerDataPartsVector> & res,
    const ServerDataPartsVector & data_parts,
    ServerCanMergeCallback can_merge_callback,
    size_t max_total_size_to_merge,
    bool aggressive,
    bool enable_batch_select,
    bool merge_with_ttl_allowed,
    Poco::Logger * log);
}
