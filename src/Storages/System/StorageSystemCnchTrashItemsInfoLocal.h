#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;

class StorageSystemCnchTrashItemsInfoLocal final : public shared_ptr_helper<StorageSystemCnchTrashItemsInfoLocal>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchTrashItemsInfoLocal>;

public:
    std::string getName() const override { return "StorageSystemCnchTrashItemsInfoLocal"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    explicit StorageSystemCnchTrashItemsInfoLocal(const StorageID & table_id_);
};

}
