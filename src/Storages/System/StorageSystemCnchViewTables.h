#pragma once

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemCnchViewTables : public shared_ptr_helper<StorageSystemCnchViewTables>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchViewTables>;
public:
    std::string getName() const override { return "SystemCnchViewTables"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;

 protected:
    StorageSystemCnchViewTables(const StorageID & table_id_);
};

}
