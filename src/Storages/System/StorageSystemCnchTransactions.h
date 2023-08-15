#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemCnchTransactions : public shared_ptr_helper<StorageSystemCnchTransactions>,
                                      public IStorageSystemOneBlock<StorageSystemCnchTransactions>
{
public:
    std::string getName() const override { return "SystemCnchTransactions"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

};

}
