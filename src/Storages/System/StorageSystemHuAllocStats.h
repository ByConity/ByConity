#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

class StorageSystemHuAllocStats final : public shared_ptr_helper<StorageSystemHuAllocStats>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemHuAllocStats>;
public:
    explicit StorageSystemHuAllocStats(const StorageID & table_id_);

    std::string getName() const override { return "SystemHuAllocStats"; }

    static NamesAndTypesList getNamesAndTypes();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

//    bool supportsTransactions() const override { return true; }
};

}
