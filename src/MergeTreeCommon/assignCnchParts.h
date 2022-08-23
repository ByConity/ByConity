#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Common/ConsistentHashUtils/ConsistentHashRing.h>

namespace DB
{

using WorkerList = std::vector<String>;
using ServerAssignmentMap = std::unordered_map<String, ServerDataPartsVector>;

ServerAssignmentMap assignCnchParts(const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts);

bool isCnchBucketTable(const ContextPtr & context, const IStorage & storage, const ServerDataPartsVector & parts);
ServerAssignmentMap assignCnchPartsForBucketTable(const ServerDataPartsVector & parts, WorkerList workers);

}
