#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Protos/DataModelHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <common/types.h>

namespace DB {

/// DataModel Adapter
///
/// Unifies the interface of all data models including:
/// - MergeTreeDataPartCNCH
/// - IMergeTreeDataPart
/// - DeleteBitmapMetaPtr
/// - ServerDataPartPtr
/// - Part/Delete Bitmaps Names (String)
class AdapterInterface
{
public:
    virtual String getPartitionId() = 0;
    virtual String getName() = 0;
    virtual ~AdapterInterface() = default;
};

class DataPartAdapter : AdapterInterface
{
    const std::shared_ptr<const MergeTreeDataPartCNCH> & data;

public:
    explicit DataPartAdapter(const std::shared_ptr<const MergeTreeDataPartCNCH> & x) : data(x) { }
    String getPartitionId() override { return data->info.partition_id; }
    String getName() override { return data->name; }
};

class IMergeTreeDataPartAdapter : AdapterInterface
{
    const IMergeTreeDataPartPtr & data;

public:
    explicit IMergeTreeDataPartAdapter(const IMergeTreeDataPartPtr & x) : data(x) { }
    String getPartitionId() override { return data->info.partition_id; }
    String getName() override { return data->info.getPartName(); }
};

class DeleteBitmapAdapter : AdapterInterface
{
    const DeleteBitmapMetaPtr data;

public:
    explicit DeleteBitmapAdapter(const DeleteBitmapMetaPtr & x) : data(x) { }
    DeleteBitmapAdapter(const MergeTreeMetaBase & /*_storage*/, const DeleteBitmapMetaPtr & x) : data(x) { }
    DeleteBitmapAdapter(const MergeTreeMetaBase & storage, const DataModelDeleteBitmapPtr & x)
        : data(std::make_shared<DeleteBitmapMeta>(storage, x))
    {
    }
    String getPartitionId() override { return data->getPartitionID(); }
    String getName() override { return dataModelName(*data->getModel()); }
    DataModelDeleteBitmapPtr getData() { return data->getModel(); }
    DeleteBitmapMetaPtr toData() { return data; }
    size_t getCommitTime() { return data->getCommitTime(); }
};

class ServerDataPartAdapter : AdapterInterface
{
    const ServerDataPartPtr data;

public:
    explicit ServerDataPartAdapter(const ServerDataPartPtr & x) : data(x) { }
    explicit ServerDataPartAdapter(const std::shared_ptr<ServerDataPartPtr> & x) : data(*x) { }
    ServerDataPartAdapter(const MergeTreeMetaBase & storage, const DB::Protos::DataModelPart && x)
        : data(std::make_shared<ServerDataPart>(createPartWrapperFromModel(storage, std::move(x))))
    {
    }
    explicit ServerDataPartAdapter(const MergeTreeMetaBase & /*storage*/, const DataModelPartWrapperPtr & x)
        : data(std::make_shared<ServerDataPart>(x))
    {
    }
    String getPartitionId() override { return data->info().partition_id; }
    String getName() override { return data->name(); }
    DataModelPartWrapperPtr getData() const { return data->part_model_wrapper; }
    size_t getCommitTime() const { return data->getCommitTime(); }
    ServerDataPartPtr toData() { return data; }
    String getPartitionMinmax() { return data->part_model().partition_minmax(); }
};


class PartPlainTextAdapter : AdapterInterface
{
    const MergeTreePartInfo part_info;

public:
    explicit PartPlainTextAdapter(const String & part_name, const MergeTreeDataFormatVersion & version)
        : part_info(MergeTreePartInfo::fromPartName(part_name, version))
    {
    }
    String getPartitionId() override { return part_info.partition_id; }
    String getName() override { return part_info.getPartName(); }
};

class DeleteBitmapPlainTextAdapter : AdapterInterface
{
    const String name;
    String partition_id;

public:
    explicit DeleteBitmapPlainTextAdapter(const String & bitmap_name) : name(bitmap_name) { }
    String getPartitionId() override;
    String getName() override { return name; }
};

}

