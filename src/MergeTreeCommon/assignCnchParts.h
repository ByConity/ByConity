#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/Hive/HiveDataPart_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Common/ConsistentHashUtils/ConsistentHashRing.h>

namespace DB
{

using WorkerList = std::vector<String>;
using ServerAssignmentMap = std::unordered_map<String, ServerDataPartsVector>;
using HivePartsAssignMap = std::unordered_map<String, HiveDataPartsCNCHVector>;
using AssignmentMap = std::unordered_map<String, MergeTreeDataPartsCNCHVector>;
using BucketNumbersAssignmentMap = std::unordered_map<String, std::set<Int64>>;

struct BucketNumberAndServerPartsAssignment
{
    ServerAssignmentMap parts_assignment_map;
    BucketNumbersAssignmentMap bucket_number_assignment_map;
};

// the hive has different allocate logic, thus separate it.
HivePartsAssignMap assignCnchHiveParts(const WorkerGroupHandle & worker_group, const HiveDataPartsCNCHVector & parts);

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchParts(const WorkerGroupHandle & worker_group, const DataPartsCnchVector & parts);

bool isCnchBucketTable(const ContextPtr & context, const IStorage & storage, const ServerDataPartsVector & parts);
BucketNumberAndServerPartsAssignment assignCnchPartsForBucketTable(const ServerDataPartsVector & parts, WorkerList workers, std::set<Int64> required_bucket_numbers = {});

}
