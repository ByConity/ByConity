#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemCnchTransactionCleanTasks final: public shared_ptr_helper<StorageSystemCnchTransactionCleanTasks>,
    public IStorageSystemOneBlock<StorageSystemCnchTransactionCleanTasks>
{
public:
    std::string getName() const override { return "StorageSystemCnchTransactionCleanTasks"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemCnchTransactionCleanTasks>;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns& res_columns, ContextPtr context,
        const SelectQueryInfo & query_info) const override;

};

}
