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

#pragma once

#include <Protos/data_models.pb.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Core/Types.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include "Storages/MergeTree/IMergeTreeDataPart_fwd.h"
#include <vector>
#include <forward_list>

namespace DB
{

/**
 * The class `DataModelPartWrapper` is to reduce the deserialize cost during read
 * It will persist needed variables which are needed by the query execution and deserialized from part model
 * It is also the object that cached
 */
class DataModelPartWrapper
{
public:
    DataModelPartWrapper();
    std::shared_ptr<Protos::DataModelPart> part_model;
    String name;

    MergeTreePartition partition;
    std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> minmax_idx;

    std::shared_ptr<MergeTreePartInfo> info;

    /// Get the txn id of the part.
    /// For a normal insertion, the DataModelPart::txnID and DataModelPart::part_info::mutation are always the same.
    /// But in some special cases(like ATTACH PARTITION, see CnchAttachProcessor::prepareParts),
    /// the DataModelPart::txnID not equal to DataModelPart::part_info::mutation.
    /// To handle this case, we need to set txnID and part_info::mutation separately, see DataModelHelpers::fillPartsModel.
    /// And when getting txnID, use DataModelPart::txnID by default, and use mutation as fallback.
    inline UInt64 txnID() const { return part_model->txnid() ? part_model->txnid() : part_model->part_info().mutation(); }
};

class DataPartInterface
{
public:
    virtual bool isServerDataPart() const = 0;
    virtual ~DataPartInterface() = default;
};

/**
 * `ServerDataPart` is for the easy usage of query execution
 * Some query level variables, such as part dependency chain and delete bitmaps, are stored in this class
 * It also provide some helper methods to act like `MergeTreeDataPart`
 */
class ServerDataPart : public std::enable_shared_from_this<ServerDataPart>, public DataPartInterface
{
public:
    explicit ServerDataPart(const DataModelPartWrapperPtr & part_model_wrapper_) : part_model_wrapper(part_model_wrapper_) { }
    explicit ServerDataPart(DataModelPartWrapperPtr && part_model_wrapper_) : part_model_wrapper(part_model_wrapper_) { }

    virtual bool isServerDataPart() const override { return true; }

    DataModelPartWrapperPtr part_model_wrapper;

    mutable std::forward_list<DataModelDeleteBitmapPtr> delete_bitmap_metas;

    UInt64 deletedRowsCount(const MergeTreeMetaBase & storage) const;

    const ImmutableDeleteBitmapPtr & getDeleteBitmap(const MergeTreeMetaBase & storage, bool is_unique_new_part) const;

    UInt64 getCommitTime() const;
    void setCommitTime(const UInt64 & new_commit_time) const;
    UInt64 getColumnsCommitTime() const;
    UInt64 getMutationCommitTime() const;
    UInt64 getEndTime() const;
    UInt64 getLastModificationTime() const;
    void setEndTime(UInt64 end_time) const;

    bool containsExactly(const ServerDataPart & other) const;

    const ServerDataPartPtr & getPreviousPart() const;
    const ServerDataPartPtr & tryGetPreviousPart() const;
    void setPreviousPart(const ServerDataPartPtr & part) const;
    ServerDataPartPtr getBasePart() const;

    bool isEmpty() const;
    UInt64 rowsCount() const;
    UInt64 rowExistsCount() const;
    UInt64 size() const;
    bool isPartial() const;
    bool isDropRangePart() const;
    bool deleted() const;
    const Protos::DataModelPart & part_model() const;
    const std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> & minmax_idx() const;
    UInt64 txnID() const;
    bool hasStagingTxnID() const;

    const MergeTreePartInfo & info() const;
    const String & name() const;
    const MergeTreePartition & partition() const;

    decltype(auto) get_name() const { return name(); }
    decltype(auto) get_info() const { return info(); }
    decltype(auto) get_partition() const { return partition(); }
    decltype(auto) get_deleted() const { return deleted(); }
    decltype(auto) get_commit_time() const { return getCommitTime(); }
    UUID get_uuid() const;

    void serializePartitionAndMinMaxIndex(const MergeTreeMetaBase & storage, WriteBuffer & buf) const;
    void serializeDeleteBitmapMetas(const MergeTreeMetaBase & storage, WriteBuffer & buffer) const;

    MutableMergeTreeDataPartCNCHPtr toCNCHDataPart(
        const MergeTreeMetaBase & storage,
        /*const std::unordered_map<UInt32, String> & id_full_paths,*/
        const std::optional<std::string> & relative_path = std::nullopt) const;

    void setVirtualPartSize(const UInt64 & virtual_part_size) const;
    UInt64 getVirtualPartSize() const;

    void setHostPort(const String & disk_cache_host_port_, const String & assign_compute_host_port_) const
    {
        disk_cache_host_port = disk_cache_host_port_;
        assign_compute_host_port = assign_compute_host_port_;
    }

    mutable String disk_cache_host_port;
    mutable String assign_compute_host_port;

private:
    mutable std::optional<UInt64> commit_time;
    mutable std::optional<UInt64> last_modification_time;
    mutable ServerDataPartPtr prev_part;
    mutable UInt64 virtual_part_size = 0;
    mutable ImmutableDeleteBitmapPtr delete_bitmap;

};

}
