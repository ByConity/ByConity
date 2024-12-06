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

#include <MergeTreeCommon/CnchServerTopology.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Protos/RPCHelpers.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Common/HostWithPorts.h>
// #include <Transaction/ICnchTransaction.h>
#include <memory>
#include <Catalog/DataModelPartWrapper.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Protos/data_models.pb.h>
#include <Storages/DataPart_fwd.h>
#include <Transaction/LockRequest.h>
#include <Transaction/TxnTimestamp.h>
#include <google/protobuf/repeated_field.h>

namespace DB
{
using DataPartInfoPtr = std::shared_ptr<MergeTreePartInfo>;
using DataModelPartPtr = std::shared_ptr<Protos::DataModelPart>;
using DataModelPartPtrVector = std::vector<DataModelPartPtr>;

struct DataModelPartWithName
{
    DataModelPartWithName(std::string && name_, DataModelPartPtr && model_) : name(std::move(name_)), model(std::move(model_)) {}
    std::string name;
    DataModelPartPtr model;
};

using DataModelPartWithNamePtr = std::shared_ptr<DataModelPartWithName>;
using DataModelPartWithNameVector = std::vector<DataModelPartWithNamePtr>;

namespace pb = google::protobuf;

MutableMergeTreeDataPartCNCHPtr createPartFromModelCommon(
    const MergeTreeMetaBase & storage, const Protos::DataModelPart & part_model, std::optional<std::string> relative_path = std::nullopt);

MutableMergeTreeDataPartCNCHPtr createPartFromModel(
    const MergeTreeMetaBase & storage, const Protos::DataModelPart & part_model, std::optional<std::string> relative_path = std::nullopt);

DataPartInfoPtr createPartInfoFromModel(const Protos::DataModelPartInfo & part_info_model);

/// TODO: All operation on vector can run in parallel

template <class T>
inline std::vector<T> createPartVectorFromModels(
    const MergeTreeMetaBase & storage,
    const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    const pb::RepeatedPtrField<std::string> * paths = nullptr)
{
    std::vector<T> res;
    res.reserve(parts_model.size());
    for (int i = 0; i < parts_model.size(); ++i)
        res.emplace_back(createPartFromModel(storage, parts_model[i], (paths ? std::optional(paths->Get(i)) : std::nullopt)));
    return res;
}

void fillPartModel(const IStorage & storage, const IMergeTreeDataPart & part, Protos::DataModelPart & part_model, bool ignore_column_commit_time = false, UInt64 txn_id = 0);

void fillPartInfoModel(const IMergeTreeDataPart & part, Protos::DataModelPartInfo & part_info_model);
void fillPartTTLInfoModel(const IMergeTreeDataPart & part, Protos::DataModelPartTTLInfo & part_ttl_info_model);

template <class T>
inline void fillPartsModel(const IStorage & storage, const std::vector<T> & parts, pb::RepeatedPtrField<Protos::DataModelPart> & parts_model, UInt64 txn_id = 0)
{
    std::for_each(parts.begin(), parts.end(), [&](const T & part)
    {
        fillPartModel(storage, *part, *parts_model.Add(), false, txn_id);
    });
}

template <class T>
inline void fillPartsInfoModel(const std::vector<T> & parts, pb::RepeatedPtrField<Protos::DataModelPartInfo> & part_infos_model)
{
    std::for_each(parts.begin(), parts.end(), [&](const T & part) { fillPartInfoModel(*part, *part_infos_model.Add()); });
}

void fillPartsModelForSend(
    const IStorage & storage, const ServerDataPartsVector & parts, pb::RepeatedPtrField<Protos::DataModelPart> & parts_model);

void fillPartsModelForSend(
    const IStorage & storage, const ServerVirtualPartVector & parts, pb::RepeatedPtrField<Protos::DataModelVirtualPart> & parts_model);

template <class T>
inline void
fillPartsModelForSend(const IStorage & storage, const std::vector<T> & parts, pb::RepeatedPtrField<Protos::DataModelPart> & parts_model)
{
    std::set<UInt64> sent_columns_commit_time;
    std::for_each(parts.begin(), parts.end(), [&](const T & part) {
        auto & part_model = *parts_model.Add();
        fillPartModel(storage, *part, part_model);
        if (part_model.has_columns_commit_time() && sent_columns_commit_time.count(part_model.columns_commit_time()) == 0)
        {
            part_model.set_columns(part->columns_ptr->toString());
            sent_columns_commit_time.insert(part_model.columns_commit_time());
        }
    });
}

template <class T>
inline void fillBasePartAndDeleteBitmapModels(
    const IStorage & storage,
    const std::vector<T> & parts,
    pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    pb::RepeatedPtrField<Protos::DataModelDeleteBitmap> & bitmaps_model)
{
    fillPartsModelForSend(storage, parts, parts_model);
    for (auto & part : parts)
    {
        for (auto & bitmap_meta : part->delete_bitmap_metas)
        {
            bitmaps_model.Add()->CopyFrom(*bitmap_meta);
        }
    }
}

inline void
fillTopologyVersions(const std::list<CnchServerTopology> & topologies, pb::RepeatedPtrField<Protos::DataModelTopology> & topology_versions)
{
    std::for_each(topologies.begin(), topologies.end(), [&](const auto & topology) {
        auto & topology_version = *topology_versions.Add();
        topology_version.set_term(topology.getTerm());
        topology_version.set_initialtime(topology.getInitialTime());
        topology_version.set_expiration(topology.getExpiration());
        for (const auto & [k, v] : topology.getVwTopologies())
        {
            auto & vw_topology = *topology_version.add_vw_topologies();
            vw_topology.set_server_vw_name(k);
            for (const auto & host_with_port : v.getServerList())
            {
                auto & server = *vw_topology.add_servers();
                server.set_hostname(host_with_port.id);
                server.set_host(host_with_port.getHost());
                server.set_rpc_port(host_with_port.rpc_port);
                server.set_tcp_port(host_with_port.tcp_port);
                server.set_http_port(host_with_port.http_port);
            }
        }
        if (!topology.getLeaderInfo().empty())
        {
            topology_version.set_leader_info(topology.getLeaderInfo());
        }
        if (!topology.getReason().empty())
        {
            topology_version.set_reason(topology.getReason());
        }
    });
}

inline std::list<CnchServerTopology>
createTopologyVersionsFromModel(const pb::RepeatedPtrField<Protos::DataModelTopology> & topology_versions)
{
    std::list<CnchServerTopology> res;
    std::for_each(topology_versions.begin(), topology_versions.end(), [&](const auto & model)
    {
        auto topology = CnchServerTopology();
        topology.setExpiration(model.expiration());
        // to be comparable with old format topology during upgrade
        if (model.has_initialtime())
            topology.setInitialTime(model.initialtime());
        if (model.has_term())
            topology.setTerm(model.term());
        for (const auto & vw_topology : model.vw_topologies())
        {
            String vw_name = vw_topology.server_vw_name();
            for (const auto & server : vw_topology.servers())
            {
                HostWithPorts host_with_port{
                    server.host(),
                    static_cast<uint16_t>(server.rpc_port()),
                    static_cast<uint16_t>(server.tcp_port()),
                    static_cast<uint16_t>(server.http_port()),
                    server.hostname()};
              host_with_port.id = server.hostname();
                topology.addServer(host_with_port, vw_name);
            }
        }
        if (model.has_leader_info())
        {
            topology.setLeaderInfo(model.leader_info());
        }
        if (model.has_reason())
        {
            topology.setReason(model.reason());
        }
        res.push_back(topology);
    });
    return res;
}

template <class T>
inline std::vector<T> createPartVectorFromModelsForSend(
    const MergeTreeMetaBase & storage,
    const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    const pb::RepeatedPtrField<std::string> * paths = nullptr)
{
    std::vector<T> res;
    res.reserve(parts_model.size());
    std::map<UInt64, NamesAndTypesListPtr> columns_versions;
    for (int i = 0; i < parts_model.size(); ++i)
    {
        const auto & part_model = parts_model[i];
        auto part = createPartFromModelCommon(storage, part_model, (paths ? std::optional(paths->Get(i)) : std::nullopt));
        part->columns_commit_time = part_model.columns_commit_time();
        if (part_model.has_columns())
        {
            part->setColumns(NamesAndTypesList::parse(part_model.columns()));
            if (part_model.has_columns_commit_time())
            {
                columns_versions[part_model.columns_commit_time()] = part->getColumnsPtr();
            }
        }
        else
        {
            part->setColumnsPtr(columns_versions[part_model.columns_commit_time()]);
        }
        res.emplace_back(std::move(part));
    }
    return res;
}

template <class T>
inline std::vector<T> createBasePartAndDeleteBitmapFromModelsForSend(
    const MergeTreeMetaBase & storage,
    const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    const pb::RepeatedPtrField<Protos::DataModelDeleteBitmap> & bitmaps_model,
    const pb::RepeatedPtrField<std::string> * paths = nullptr)
{
    std::vector<T> res = createPartVectorFromModelsForSend<T>(storage, parts_model, paths);

    auto bitmap_it = bitmaps_model.begin();
    auto same_block = [](const Protos::DataModelDeleteBitmap & bitmap, const T & part) {
        return bitmap.partition_id() == part->info.partition_id && bitmap.part_min_block() == part->info.min_block
            && bitmap.part_max_block() == part->info.max_block;
    };
    /// fill in bitmap metas for each part
    for (auto & part : res)
    {
        /// partial parts don't have bitmap.
        if (bitmap_it == bitmaps_model.end() || !same_block(*bitmap_it, part))
            continue;

        auto list_it = part->delete_bitmap_metas.before_begin();
        do
        {
            list_it = part->delete_bitmap_metas.insert_after(list_it, std::make_shared<Protos::DataModelDeleteBitmap>(*bitmap_it));
            bitmap_it++;
        } while (bitmap_it != bitmaps_model.end() && same_block(*bitmap_it, part));
    }
    return res;
}

template <class T>
inline std::vector<T> createPartVectorFromModelsForSend(
    const MergeTreeMetaBase & storage,
    const pb::RepeatedPtrField<Protos::DataModelVirtualPart> & parts_model,
    const pb::RepeatedPtrField<std::string> * paths = nullptr)
{
    std::vector<T> res;
    res.reserve(parts_model.size());
    std::map<UInt64, NamesAndTypesListPtr> columns_versions;
    for (int i = 0; i < parts_model.size(); ++i)
    {
        const auto & part_model = parts_model[i].part();
        auto part = createPartFromModelCommon(storage, part_model, (paths ? std::optional(paths->Get(i)) : std::nullopt));
        part->columns_commit_time = part_model.columns_commit_time();
        if (part_model.has_columns())
        {
            part->setColumns(NamesAndTypesList::parse(part_model.columns()));
            if (part_model.has_columns_commit_time())
            {
                columns_versions[part_model.columns_commit_time()] = part->getColumnsPtr();
            }
        }
        else
        {
            part->setColumnsPtr(columns_versions[part_model.columns_commit_time()]);
        }

        const auto & ranges = parts_model[i].mark_ranges();
        if (!ranges.empty())
        {
            part->mark_ranges_for_virtual_part = std::make_unique<MarkRanges>();
            for (int j = 0; j < ranges.size(); j += 2)
            {
                part->mark_ranges_for_virtual_part->emplace_back(ranges[j], ranges[j + 1]);
            }
        }
        res.emplace_back(std::move(part));
    }
    return res;
}

template <class T>
inline std::vector<T> createBasePartAndDeleteBitmapFromModelsForSend(
    const MergeTreeMetaBase & storage,
    const pb::RepeatedPtrField<Protos::DataModelVirtualPart> & parts_model,
    const pb::RepeatedPtrField<Protos::DataModelDeleteBitmap> & bitmaps_model,
    const pb::RepeatedPtrField<std::string> * paths = nullptr)
{
    std::vector<T> res = createPartVectorFromModelsForSend<T>(storage, parts_model, paths);

    auto bitmap_it = bitmaps_model.begin();
    auto same_block = [](const Protos::DataModelDeleteBitmap & bitmap, const T & part) {
        return bitmap.partition_id() == part->info.partition_id && bitmap.part_min_block() == part->info.min_block
            && bitmap.part_max_block() == part->info.max_block;
    };
    /// fill in bitmap metas for each part
    for (auto & part : res)
    {
        /// partial parts don't have bitmap.
        if (bitmap_it == bitmaps_model.end() || !same_block(*bitmap_it, part))
            continue;

        auto list_it = part->delete_bitmap_metas.before_begin();
        do
        {
            list_it = part->delete_bitmap_metas.insert_after(list_it, std::make_shared<Protos::DataModelDeleteBitmap>(*bitmap_it));
            bitmap_it++;
        } while (bitmap_it != bitmaps_model.end() && same_block(*bitmap_it, part));
    }
    return res;
}

inline DataModelPartPtr createPtrFromModel(Protos::DataModelPart part_model)
{
    return std::make_shared<Protos::DataModelPart>(std::move(part_model));
}

std::shared_ptr<MergeTreePartition> createPartitionFromMetaModel(const MergeTreeMetaBase & storage, const Protos::PartitionMeta & meta);

std::shared_ptr<MergeTreePartition> createPartitionFromMetaString(const MergeTreeMetaBase & storage, const String & parition_minmax_info);

inline DeleteBitmapMetaPtr createFromModel(const MergeTreeMetaBase & storage, const Protos::DataModelDeleteBitmap & model)
{
    auto model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>(model);
    return std::make_shared<DeleteBitmapMeta>(storage, model_ptr);
}

void fillLockInfoModel(const LockInfo & lock_info, Protos::DataModelLockInfo & model);
LockInfoPtr createLockInfoFromModel(const Protos::DataModelLockInfo & model);

DataModelPartWrapperPtr createPartWrapperFromModel(const MergeTreeMetaBase & storage, const Protos::DataModelPart && part_model, const String && part_name = "");

DataModelPartWrapperPtr createPartWrapperFromModelBasic(const Protos::DataModelPart && part_model, const String && part_name = "");

ServerDataPartPtr createServerPartFromDataPart(const MergeTreeMetaBase & storage, const IMergeTreeDataPartPtr & part);

ServerDataPartsVector
createServerPartsFromModels(const MergeTreeMetaBase & storage, const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model);

ServerDataPartsVector createServerPartsFromDataParts(const MergeTreeMetaBase & storage, const MergeTreeDataPartsCNCHVector & parts);
ServerDataPartsVector createServerPartsFromDataParts(const MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & parts);

IMergeTreeDataPartsVector createPartVectorFromServerParts(
    const MergeTreeMetaBase & storage,
    const ServerDataPartsVector & parts);

size_t fillCnchFilePartsModel(const FileDataPartsCNCHVector & parts, pb::RepeatedPtrField<Protos::CnchFilePartModel> & parts_model);
FileDataPartsCNCHVector createCnchFileDataParts(const ContextPtr & context, const pb::RepeatedPtrField<Protos::CnchFilePartModel> & parts_model);

String getServerVwNameFrom(const Protos::DataModelTable & model);
String getServerVwNameFrom(const Protos::TableIdentifier & model);

}
