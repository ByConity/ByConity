#pragma once
#include "Storages/StorageSnapshot.h"
#include "config_core.h"

#if USE_MYSQL

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemCnchMaterializedMySQL : public shared_ptr_helper<StorageSystemCnchMaterializedMySQL>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchMaterializedMySQL>;
public:
    std::string getName() const override { return "SystemCnchMaterializedMySQL"; }

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemCnchMaterializedMySQL(const StorageID & table_id_);
};

} /// namespace DB

#endif
