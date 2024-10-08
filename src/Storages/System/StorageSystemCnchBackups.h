#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemCnchBackups : public shared_ptr_helper<StorageSystemCnchBackups>, public IStorageSystemOneBlock<StorageSystemCnchBackups>
{
public:
    std::string getName() const override
    {
        return "SystemCnchBackups";
    }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
