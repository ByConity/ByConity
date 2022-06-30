#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemCnchDatabases : public shared_ptr_helper<StorageSystemCnchDatabases>, public IStorageSystemOneBlock<StorageSystemCnchDatabases>
{
public:
    std::string getName() const override
    {
        return "SystemCnchDatabases";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
