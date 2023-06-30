#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{


class StorageSystemCloudTables final : public shared_ptr_helper<StorageSystemCloudTables>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCloudTables>;
public:
    std::string getName() const override { return "SystemTables"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    explicit StorageSystemCloudTables(const StorageID & table_id_);
};

}
