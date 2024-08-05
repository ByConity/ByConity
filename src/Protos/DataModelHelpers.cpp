/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <memory>
#include <optional>
#include <Protos/DataModelHelpers.h>

#include <memory>
#include <Catalog/DataModelPartWrapper.h>
#include <Disks/DiskHelpers.h>
#include <Disks/HDFS/DiskByteHDFS.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/RPCHelpers.h>
#include <Protos/data_models.pb.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Transaction/TxnTimestamp.h>
#include "common/logger_useful.h"
#include <Common/Exception.h>
#include <common/JSON.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/StorageCnchMergeTree.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Disks/HDFS/DiskByteHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TTL_IN_DATA_MODEL_PART;
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int EMPTY_PARTITION_IN_DATA_MODEL_PART;
}

DataModelPartWrapperPtr createPartWrapperFromModel(const MergeTreeMetaBase & storage, const Protos::DataModelPart && part_model, const String && part_name)
{
    DataModelPartWrapperPtr part_model_wrapper = createPartWrapperFromModelBasic(std::move(part_model), std::move(part_name));

    /// Partition and Minmax index
    ReadBufferFromString partition_minmax_buf(part_model_wrapper->part_model->partition_minmax());
    if (unlikely(storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
        throw Exception("MergeTree data format is too old", ErrorCodes::FORMAT_VERSION_TOO_OLD);

    part_model_wrapper->partition->load(storage, partition_minmax_buf);
    if (part_model_wrapper->part_model->rows_count() > 0)
    {
        part_model_wrapper->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
        part_model_wrapper->minmax_idx->load(storage, partition_minmax_buf);
    }

    return part_model_wrapper;
}

DataModelPartWrapperPtr createPartWrapperFromModelBasic(const Protos::DataModelPart && part_model, const String && part_name)
{
    DataModelPartWrapperPtr part_model_wrapper = std::make_shared<DataModelPartWrapper>();

    part_model_wrapper->info = createPartInfoFromModel(part_model.part_info());
    if (part_name.empty())
        part_model_wrapper->name = part_model_wrapper->info->getPartName();
    else
        part_model_wrapper->name = std::move(part_name);

    part_model_wrapper->part_model = std::make_shared<Protos::DataModelPart>(std::move(part_model));
    auto & inside_part_model = *(part_model_wrapper->part_model);
    if (!inside_part_model.has_deleted())
        inside_part_model.set_deleted(false);
    if (!inside_part_model.has_data_path_id())
        inside_part_model.set_data_path_id(0);
    if (!inside_part_model.has_mutation_commit_time())
        inside_part_model.set_mutation_commit_time(0);
    if (!inside_part_model.has_commit_time())
        inside_part_model.set_commit_time(part_model_wrapper->info->mutation);

    if (inside_part_model.has_min_unique_key() && inside_part_model.min_unique_key().empty() && inside_part_model.rows_count() > 0)
        throw Exception("min unique key of non empty part must be non empty", ErrorCodes::LOGICAL_ERROR);
    if (inside_part_model.has_max_unique_key() && inside_part_model.max_unique_key().empty() && inside_part_model.rows_count() > 0)
        throw Exception("max unique key of non empty part must be non empty", ErrorCodes::LOGICAL_ERROR);

    return part_model_wrapper;
}

MutableMergeTreeDataPartCNCHPtr
createPartFromModelCommon(const MergeTreeMetaBase & storage, const Protos::DataModelPart & part_model, std::optional<String> relative_path)
{
    /// Create part object
    auto info = createPartInfoFromModel(part_model.part_info());
    String part_name = info->getPartName();
    UInt32 path_id = part_model.has_data_path_id() ? part_model.data_path_id() : 0;

    DiskPtr remote_disk = getDiskForPathId(storage.getStoragePolicy(IStorage::StorageLocation::MAIN), path_id);
    auto mock_volume = std::make_shared<SingleDiskVolume>("volume_mock", remote_disk, 0);
    UUID part_id = UUIDHelpers::Nil;
    switch(remote_disk->getType())
    {
        case DiskType::Type::ByteS3:
        {
            part_id = RPCHelpers::createUUID(part_model.part_id());
            if (!relative_path.has_value())
                relative_path = UUIDHelpers::UUIDToString(part_id);
            break;
        }
        case DiskType::Type::ByteHDFS:
        {
            if (!relative_path.has_value())
                relative_path = info->getPartNameWithHintMutation();
            break;
        }
        default:
            throw Exception(fmt::format("Unsupported disk type {} in createPartFromModelCommon",
                DiskType::toString(remote_disk->getType())), ErrorCodes::LOGICAL_ERROR);
    }
    auto part = std::make_shared<MergeTreeDataPartCNCH>(storage, part_name, *info,
        mock_volume, relative_path, nullptr, part_id);

    if (part_model.has_staging_txn_id())
    {
        part->staging_txn_id = part_model.staging_txn_id();
        if (remote_disk->getType() == DiskType::Type::ByteHDFS)
        {
            /// this part shares the same relative path with the corresponding staged part
            MergeTreePartInfo staged_part_info = part->info;
            staged_part_info.mutation = part->staging_txn_id;
            part->relative_path = staged_part_info.getPartNameWithHintMutation();
        }
    }

    part->bytes_on_disk = part_model.size();
    part->rows_count = part_model.rows_count();
    part->row_exists_count = part_model.has_row_exists_count() ? part_model.row_exists_count() : part_model.rows_count();
    if (!part_model.has_marks_count())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cnch parts must have mark count");
    if (!part->isPartial() || !part->isEmpty())
    {
        /// Partial & empty part will be load later
        std::vector<size_t> index_granularities(part_model.index_granularities().begin(), part_model.index_granularities().end());
        part->loadIndexGranularity(part_model.marks_count(), index_granularities);
    }
    part->deleted = part_model.has_deleted() && part_model.deleted();
    part->delete_flag = part_model.has_delete_flag() && part_model.delete_flag();
    part->low_priority = part_model.has_low_priority() && part_model.low_priority();
    part->bucket_number = part_model.bucket_number();
    part->table_definition_hash = part_model.table_definition_hash();
    part->mutation_commit_time = part_model.has_mutation_commit_time() ? part_model.mutation_commit_time() : 0;
    part->last_modification_time = part_model.last_modification_time();
    if (part_model.has_commit_time())
        part->commit_time = TxnTimestamp{part_model.commit_time()};
    else
        part->commit_time = TxnTimestamp{static_cast<UInt64>(info->mutation)};

    if (part_model.has_min_unique_key())
    {
        part->min_unique_key = part_model.min_unique_key();
        if (part->rows_count > 0 && part->min_unique_key.empty())
            throw Exception("min unique key of non empty part must be non empty", ErrorCodes::LOGICAL_ERROR);
    }
    if (part_model.has_max_unique_key())
    {
        part->max_unique_key = part_model.max_unique_key();
        if (part->rows_count > 0 && part->max_unique_key.empty())
            throw Exception("max unique key of non empty part must be non empty", ErrorCodes::LOGICAL_ERROR);
    }

    /// Partition and Minmax index
    ReadBufferFromString partition_minmax_buf(part_model.partition_minmax());
    part->loadPartitionAndMinMaxIndex(partition_minmax_buf);

    part->secondary_txn_id = part_model.has_secondary_txn_id() ? TxnTimestamp{part_model.secondary_txn_id()} : TxnTimestamp{0};
    part->virtual_part_size = part_model.has_virtual_part_size() ? part_model.virtual_part_size() : 0;
    part->covered_parts_count = part_model.has_covered_parts_count() ? part_model.covered_parts_count() : 0;
    part->covered_parts_size = part_model.has_covered_parts_size() ? part_model.covered_parts_size() : 0;
    part->covered_parts_rows = part_model.has_covered_parts_rows() ? part_model.covered_parts_rows() : 0;

    std::unordered_set<std::string> projection_parts_names(part_model.projections().begin(), part_model.projections().end());
    part->setProjectionPartsNames(projection_parts_names);
    part->setHostPort(part_model.disk_cache_host_port(), part_model.assign_compute_host_port());

    part->ttl_infos.part_min_ttl = part_model.part_ttl_info().part_min_ttl();
    part->ttl_infos.part_max_ttl = part_model.part_ttl_info().part_max_ttl();
    part->ttl_infos.part_finished = part_model.part_ttl_info().part_finished();

    return part;
}

DataPartInfoPtr createPartInfoFromModel(const Protos::DataModelPartInfo & part_info_model)
{
    auto part_info_ptr = std::make_shared<MergeTreePartInfo>();
    part_info_ptr->partition_id = part_info_model.partition_id();
    part_info_ptr->min_block = part_info_model.min_block();
    part_info_ptr->max_block = part_info_model.max_block();
    part_info_ptr->level = part_info_model.level();
    part_info_ptr->mutation = part_info_model.mutation();
    part_info_ptr->hint_mutation = part_info_model.hint_mutation();
    part_info_ptr->storage_type = StorageType::ByteHDFS;
    return part_info_ptr;
}

MutableMergeTreeDataPartCNCHPtr createPartFromModel(
    const MergeTreeMetaBase & storage,
    const Protos::DataModelPart & part_model,
    /*const std::unordered_map<UInt32, String> & id_full_paths,*/ std::optional<String> relative_path)
{
    auto part = createPartFromModelCommon(storage, part_model, relative_path);
    /// Columns, required
    if (part_model.has_columns())
    {
        part->setColumns(NamesAndTypesList::parse(part_model.columns()));
        part->columns_commit_time = storage.getPartColumnsCommitTime(part->getColumns());
    }
    else
    {
        part->columns_commit_time = part_model.columns_commit_time();
        part->setColumnsPtr(storage.getPartColumns(part_model.columns_commit_time()));
    }

    // if (!id_full_paths.empty())
    // {
    //     auto iter = id_full_paths.find(part->data_path_id);
    //     if (iter == id_full_paths.end())
    //         throw Exception("data path id " + std::to_string(part->data_path_id) + " don't find", ErrorCodes::LOGICAL_ERROR);
    //     part->full_data_path = iter->second;
    // }

    return part;
}

void fillPartModel(const IStorage & storage, const IMergeTreeDataPart & part, Protos::DataModelPart & part_model, bool ignore_column_commit_time, UInt64 txn_id)
{
    /// fill part info
    Protos::DataModelPartInfo * model_info = part_model.mutable_part_info();
    fillPartInfoModel(part, *model_info);

    Protos::DataModelPartTTLInfo * model_ttl_info = part_model.mutable_part_ttl_info();
    fillPartTTLInfoModel(part, *model_ttl_info);

    part_model.set_size(part.bytes_on_disk);
    part_model.set_rows_count(part.rows_count);
    part_model.set_row_exists_count(part.row_exists_count.has_value() ? part.row_exists_count.value() : part.rows_count);
    ///TODO: if we need marks_count in ce?
    if (part.index_granularity_info.is_adaptive)
    {
        auto part_index_granularity = part.index_granularity.getIndexGranularities();
        part_model.mutable_index_granularities()->Add(part_index_granularity.begin(), part_index_granularity.end());
    }

    const auto cnch_part = std::dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part.shared_from_this());
    if (cnch_part)
        part_model.set_marks_count(cnch_part->getMarksCount());

    /// Normally, the DataModelPart::txnID and DataModelPart::part_info::mutation are always the same.
    /// But in some special cases(like ATTACH PARTITION, see CnchAttachProcessor::prepareParts),
    /// the DataModelPart::txnID not equal to DataModelPart::part_info::mutation.
    /// To handle this case, we need to set txnID and part_info::mutation separately.
    /// And when getting txnID, use DataModelPart::txnID by default, and use mutation as fallback, see ServerDataPart::txnID() .
    if (part.secondary_txn_id != IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
    {
        /// If we in a interactive transaction, use primary txn's txnid for this part
        part_model.set_txnid(part.info.mutation);
    }
    else
    {
        part_model.set_txnid(txn_id ? txn_id : part.info.mutation);
    }
    part_model.set_bucket_number(part.bucket_number);
    part_model.set_table_definition_hash(part.table_definition_hash);
    part_model.set_commit_time(part.commit_time.toUInt64());
    // TODO support multiple namenode , mock 0 now.
    part_model.set_data_path_id(0);

    if (part.deleted)
        part_model.set_deleted(part.deleted);
    if (part.mutation_commit_time)
        part_model.set_mutation_commit_time(part.mutation_commit_time);
    if (part.delete_flag)
        part_model.set_delete_flag(part.delete_flag);
    if (part.low_priority)
        part_model.set_low_priority(part.low_priority);
    if (part.last_modification_time)
        part_model.set_last_modification_time(part.last_modification_time);

    if (!ignore_column_commit_time && part.columns_commit_time)
    {
        part_model.set_columns_commit_time(part.columns_commit_time);
    }
    else if (auto columns_commit_time = storage.getPartColumnsCommitTime(*(part.getColumnsPtr())))
    {
        part_model.set_columns_commit_time(columns_commit_time);
    }
    else
    {
        /// If the parts columns not match any storage version. Store it instead of columns_commit_time
        part_model.set_columns(part.getColumns().toString());
    }

    if (storage.getInMemoryMetadataPtr()->hasDynamicSubcolumns())
    {
        if (auto commit_time = DB::getColumnsCommitTimeForJSONTable(storage, *(part.getColumnsPtr())))
            part_model.set_columns_commit_time(commit_time);

        part_model.set_columns(part.getColumns().toString());
    }

    if (!part.min_unique_key.empty())
        part_model.set_min_unique_key(part.min_unique_key);
    if (!part.max_unique_key.empty())
        part_model.set_max_unique_key(part.max_unique_key);

    WriteBufferFromString partition_minmax_out(*part_model.mutable_partition_minmax());
    part.storePartitionAndMinMaxIndex(partition_minmax_out);

    if (part.secondary_txn_id)
    {
        part_model.set_secondary_txn_id(part.secondary_txn_id.toUInt64());
    }

    if (part.staging_txn_id)
    {
        part_model.set_staging_txn_id(part.staging_txn_id);
    }

    if (part.virtual_part_size)
    {
        part_model.set_virtual_part_size(part.virtual_part_size);
    }

    if (part.covered_parts_count)
    {
        part_model.set_covered_parts_count(part.covered_parts_count);
    }

    if (part.covered_parts_size)
    {
        part_model.set_covered_parts_size(part.covered_parts_size);
    }

    if (part.covered_parts_rows)
    {
        part_model.set_covered_parts_rows(part.covered_parts_rows);
    }

    if (!part.disk_cache_host_port.empty())
    {
        part_model.set_disk_cache_host_port(part.disk_cache_host_port);
    }

    if (!part.assign_compute_host_port.empty())
    {
        part_model.set_assign_compute_host_port(part.assign_compute_host_port);
    }

    for (const auto & projection : part.getProjectionPartsNames())
        part_model.add_projections(projection);

    // For part in hdfs, it's id will be filled with 0
    RPCHelpers::fillUUID(part.get_uuid(), *(part_model.mutable_part_id()));
}

void fillPartInfoModel(const IMergeTreeDataPart & part, Protos::DataModelPartInfo & part_info_model)
{
    part_info_model.set_partition_id(part.info.partition_id);
    part_info_model.set_min_block(part.info.min_block);
    part_info_model.set_max_block(part.info.max_block);
    part_info_model.set_level(part.info.level);
    part_info_model.set_mutation(part.info.mutation);
    part_info_model.set_hint_mutation(part.info.hint_mutation);
}

void fillPartTTLInfoModel(const IMergeTreeDataPart & part, Protos::DataModelPartTTLInfo & part_ttl_info_model)
{
    part_ttl_info_model.set_part_min_ttl(part.ttl_infos.part_min_ttl);
    part_ttl_info_model.set_part_max_ttl(part.ttl_infos.part_max_ttl);
    part_ttl_info_model.set_part_finished(!part.ttl_infos.hasAnyNonFinishedTTLs());
}

void fillPartsModelForSend(
    const IStorage & storage, const ServerDataPartsVector & parts, pb::RepeatedPtrField<Protos::DataModelPart> & parts_model)
{
    std::set<UInt64> sent_columns_commit_time;
    for (const auto & part : parts)
    {
        auto & part_model = *parts_model.Add();
        part_model = part->part_model();
        part_model.set_commit_time(part->getCommitTime());
        part_model.set_virtual_part_size(part->getVirtualPartSize());

        if (storage.getInMemoryMetadataPtr()->hasDynamicSubcolumns() && part_model.has_columns())
            continue;

        if (part_model.has_columns_commit_time() && sent_columns_commit_time.count(part_model.columns_commit_time()) == 0)
        {
            auto storage_columns = storage.getPartColumns(part_model.columns_commit_time());

            part_model.set_columns(storage_columns->toString());
            sent_columns_commit_time.insert(part_model.columns_commit_time());
        }
        part_model.set_disk_cache_host_port(part->disk_cache_host_port);
        part_model.set_assign_compute_host_port(part->assign_compute_host_port);
    }
}

void fillPartsModelForSend(
    const IStorage & storage, const ServerVirtualPartVector & parts, pb::RepeatedPtrField<Protos::DataModelVirtualPart> & parts_model)
{
    std::set<UInt64> sent_columns_commit_time;
    for (const auto & part_with_ranges : parts)
    {
        const auto & part = part_with_ranges->part;
        const auto & ranges = part_with_ranges->mark_ranges;
        auto & model = *parts_model.Add();
        auto * part_model = model.mutable_part();
        *part_model = part->part_model();

        part_model->set_commit_time(part->getCommitTime());
        part_model->set_virtual_part_size(part->getVirtualPartSize());

        auto * ranges_model = model.mutable_mark_ranges();
        ranges_model->Reserve(2 * ranges->size());
        for (const auto & range : *ranges)
        {
            ranges_model->Add(range.begin);
            ranges_model->Add(range.end);
        }

        if (storage.getInMemoryMetadataPtr()->hasDynamicSubcolumns() && part_model->has_columns())
            continue;

        if (part_model->has_columns_commit_time() && sent_columns_commit_time.count(part_model->columns_commit_time()) == 0)
        {
            part_model->set_columns(storage.getPartColumns(part_model->columns_commit_time())->toString());
            sent_columns_commit_time.insert(part_model->columns_commit_time());
        }
    }
}

std::shared_ptr<MergeTreePartition> createPartitionFromMetaModel(const MergeTreeMetaBase & storage, const Protos::PartitionMeta & meta)
{
    std::shared_ptr<MergeTreePartition> partition_ptr = std::make_shared<MergeTreePartition>();
    ReadBufferFromString partition_minmax_buf(meta.partition_minmax());
    partition_ptr->load(storage, partition_minmax_buf);
    return partition_ptr;
}

std::shared_ptr<MergeTreePartition> createPartitionFromMetaString(const MergeTreeMetaBase & storage, const String & parition_minmax_info)
{
    std::shared_ptr<MergeTreePartition> partition_ptr = std::make_shared<MergeTreePartition>();
    ReadBufferFromString partition_minmax_buf(parition_minmax_info);
    partition_ptr->load(storage, partition_minmax_buf);
    return partition_ptr;
}

void fillLockInfoModel(const LockInfo & info, Protos::DataModelLockInfo & model)
{
    model.set_txn_id(info.txn_id);
    model.set_lock_mode(to_underlying(info.lock_mode));
    model.set_timeout(info.timeout);
    model.set_lock_id(info.lock_id);
    Protos::DataModelLockField * field = model.mutable_lock_field();
    field->set_table_prefix(info.table_uuid_with_prefix);
    if (info.hasBucket())
        field->set_bucket(info.bucket);
    if (info.hasPartition())
        field->set_partition(info.partition);
}

LockInfoPtr createLockInfoFromModel(const Protos::DataModelLockInfo & model)
{
    LockMode mode = static_cast<LockMode>(model.lock_mode());
    const auto & field = model.lock_field();
    Int64 bucket = field.has_bucket() ? field.bucket() : -1;
    const String & partition = field.has_partition() ? field.partition() : "";

    auto lock_info = std::make_shared<LockInfo>(model.txn_id());
    lock_info->setLockID(model.lock_id())
        .setMode(mode)
        .setTimeout(model.timeout())
        .setUUIDAndPrefixFromModel(field.table_prefix())
        .setBucket(bucket)
        .setPartition(partition);
    return lock_info;
}

ServerDataPartsVector
createServerPartsFromModels(const MergeTreeMetaBase & storage, const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model)
{
    ServerDataPartsVector res;
    res.reserve(parts_model.size());

    for (const auto & part_model : parts_model)
    {
        res.push_back(std::make_shared<ServerDataPart>(createPartWrapperFromModel(storage, Protos::DataModelPart(part_model))));
    }

    return res;
}

ServerDataPartPtr createServerPartFromDataPart(const MergeTreeMetaBase & storage, const IMergeTreeDataPartPtr & part)
{
    auto part_model = std::make_shared<Protos::DataModelPart>();
    fillPartModel(storage, *part, *part_model);

    auto res = std::make_shared<ServerDataPart>(createPartWrapperFromModel(storage, std::move(*part_model)));
    if (auto prev_part = part->tryGetPreviousPart())
        res->setPreviousPart(createServerPartFromDataPart(storage, prev_part));
    return res;
}

ServerDataPartsVector createServerPartsFromDataParts(const MergeTreeMetaBase & storage, const MergeTreeDataPartsCNCHVector & parts)
{
    ServerDataPartsVector res;
    res.reserve(parts.size());
    for (const auto & part : parts)
        res.push_back(createServerPartFromDataPart(storage, part));
    return res;
}

ServerDataPartsVector createServerPartsFromDataParts(const MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & parts)
{
    ServerDataPartsVector res;
    res.reserve(parts.size());
    for (const auto & part : parts)
        res.push_back(createServerPartFromDataPart(storage, part));
    return res;
}

IMergeTreeDataPartsVector createPartVectorFromServerParts(
    const MergeTreeMetaBase & storage, const ServerDataPartsVector & parts)
{
    IMergeTreeDataPartsVector res;
    res.reserve(parts.size());
    for (const auto & part : parts)
    {
        /// already deal with prev_part in ServerDataPart::toCNCHDataPart.
        res.push_back(part->toCNCHDataPart(storage, std::nullopt));
    }
    return res;
}

size_t fillCnchFilePartsModel(const FileDataPartsCNCHVector & parts, pb::RepeatedPtrField<Protos::CnchFilePartModel> & parts_model)
{
    for (const auto & part : parts)
    {
        auto & part_model = *parts_model.Add();
        auto & info = *part_model.mutable_part_info();
        *info.mutable_name() = part->info.name;
    }

    return parts.size();
}

FileDataPartsCNCHVector createCnchFileDataParts(const ContextPtr & /*context*/, const pb::RepeatedPtrField<Protos::CnchFilePartModel> & parts_model)
{
    FileDataPartsCNCHVector res;
    res.reserve(parts_model.size());
    for (const auto & part: parts_model)
    {
        res.emplace_back(std::make_shared<FileDataPart>(part.part_info().name()));
    }
    return res;
}

String getServerVwNameFrom(const Protos::DataModelTable & model)
{
    return model.has_server_vw_name() ? model.server_vw_name() : DEFAULT_SERVER_VW_NAME;
}

String getServerVwNameFrom(const Protos::TableIdentifier & model)
{
    return model.has_server_vw_name() ? model.server_vw_name() : DEFAULT_SERVER_VW_NAME;
}

}
