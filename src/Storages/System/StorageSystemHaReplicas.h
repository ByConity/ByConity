#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `ha_replicas` system table, which provides information about the status of the ha tables.
  */
class StorageSystemHaReplicas : public shared_ptr_helper<StorageSystemHaReplicas>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemHaReplicas>;
public:
    std::string getName() const override { return "SystemHaReplicas"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemHaReplicas(const StorageID & table_id_);
};

}
