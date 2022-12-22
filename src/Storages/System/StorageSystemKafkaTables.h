#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `kafka_tables` system table, which provides information about the status of the kafka tables.
  */
class StorageSystemKafkaTables : public shared_ptr_helper<StorageSystemKafkaTables>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemKafkaTables>;
public:
    std::string getName() const override { return "SystemKafkaTables"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    explicit StorageSystemKafkaTables(const StorageID & table_id_);
};

}

#endif
