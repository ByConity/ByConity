#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemCnchDatabasesHistory : public shared_ptr_helper<StorageSystemCnchDatabasesHistory>, public IStorageSystemOneBlock<StorageSystemCnchDatabasesHistory>
{
public:
    std::string getName() const override
    {
        return "SystemCnchDatabasesHistory";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
