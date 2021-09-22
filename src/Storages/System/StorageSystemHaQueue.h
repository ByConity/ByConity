#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `ha_queue` system table, which allows you to view the replication queues for the replicated tables.
  */
class StorageSystemHaQueue final : public ext::shared_ptr_helper<StorageSystemHaQueue>, public IStorageSystemOneBlock<StorageSystemHaQueue>
{
    friend struct ext::shared_ptr_helper<StorageSystemHaQueue>;
public:
    std::string getName() const override { return "SystemHaQueue"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
