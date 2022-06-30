#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>

namespace DB
{
class Context;

class StorageSystemCnchTableInfo : public shared_ptr_helper<StorageSystemCnchTableInfo>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchTableInfo>;
public:
    std::string getName() const override { return "SystemCnchTableInfo"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;


protected:
    StorageSystemCnchTableInfo(const StorageID & table_id_);

};

}
