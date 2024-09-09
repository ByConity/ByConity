#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;

/// Used to provide active transaction list for each table running on the CURRENT server
class StorageSystemCnchTableTransactions : public shared_ptr_helper<StorageSystemCnchTableTransactions>,
        public IStorageSystemOneBlock<StorageSystemCnchTableTransactions>
{
public:
    std::string getName() const override { return "SystemCnchTableTransactions"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
