#pragma once

#include <Core/Block.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnBitMap64.h>
#include <DataTypes/IDataType.h>

namespace DB
{

struct DictContext
{
    MergeTreeMetaBase & data;
    NamesAndTypesList & dict_column_types;
    ConstraintsDescription & constraints;
    MergeTreeDataWriter & split_writer;
    std::unordered_set<Int64> memory_bucket_ids;
};

class ReadDictInputStream;

class StorageDictCloudMergeTree final : public StorageCloudMergeTree
{
public:

    std::string getName() const override { return "DictCloud" + merging_params.getModeName() + "MergeTree"; }
    String getQualifiedTableName() const { return getDatabaseName() + "." + getTableName(); }

    StorageDictCloudMergeTree(
        const StorageID & table_id_,
        String cnch_database_name_,
        String cnch_table_name_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

    ~StorageDictCloudMergeTree() override = default;

private:
    friend class ReadDictInputStream;

    void init();

    NamesAndTypesList dict_column_types;

    using DictLock = std::shared_mutex;
    using DictReadLock = std::shared_lock<DictLock>;
    using DictWriteLock = std::unique_lock<DictLock>;

    mutable DictLock dict_lock;
    DictReadLock getDictReadLock() const { return DictReadLock(dict_lock); }
    DictWriteLock getDictWriteLock() const { return DictWriteLock(dict_lock); }

    template <typename KEY_TYPE>
    MutableColumnPtr generateKeyConstraintColumn(const ColumnBitMap64 & bitmap_column);

    TxnTimestamp last_update_timestamp{0};
    std::map<Int64, String> dict_bucket_versions;

    ConstraintsDescription constraints;

    /**
     * for splitting block into buckets
     */
    MergeTreeDataWriter split_writer;

    std::atomic<bool> first_time_load_dict {false};
};

}
