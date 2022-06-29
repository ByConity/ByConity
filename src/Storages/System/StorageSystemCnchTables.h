#pragma once

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemCnchTables : public shared_ptr_helper<StorageSystemCnchTables>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchTables>;
public:
    std::string getName() const override { return "SystemCnchTables"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;


 protected:
    StorageSystemCnchTables(const StorageID & table_id_);
};

}
