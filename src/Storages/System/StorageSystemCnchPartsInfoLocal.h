#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>

namespace DB
{

class Context;

class StorageSystemCnchPartsInfoLocal final: public shared_ptr_helper<StorageSystemCnchPartsInfoLocal>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchPartsInfoLocal>;
public:
    std::string getName() const override { return "StorageSystemCnchPartsInfoLocal"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;

protected:
    explicit StorageSystemCnchPartsInfoLocal(const StorageID & table_id_);
};

}
