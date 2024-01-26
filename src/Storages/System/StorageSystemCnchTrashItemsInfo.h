#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>

namespace DB
{

class Context;

class StorageSystemCnchTrashItemsInfo : public shared_ptr_helper<StorageSystemCnchTrashItemsInfo>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchTrashItemsInfo>;
public:
    std::string getName() const override { return "StorageSystemCnchTrashItemsInfo"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    explicit StorageSystemCnchTrashItemsInfo(const StorageID & table_id_);
};

}
