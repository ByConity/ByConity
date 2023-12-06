#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemCnchSnapshots : public shared_ptr_helper<StorageSystemCnchSnapshots>, public IStorageSystemOneBlock<StorageSystemCnchSnapshots>
{
public:
    std::string getName() const override
    {
        return "SystemCnchSnapshots";
    }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
