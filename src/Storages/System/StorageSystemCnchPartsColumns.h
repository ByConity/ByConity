#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

class StorageSystemCnchPartsColumns : public shared_ptr_helper<StorageSystemCnchPartsColumns>, public IStorageSystemOneBlock<StorageSystemCnchPartsColumns>
{
public:

    std::string getName() const override
    {
        return "SystemCnchPartsColumns";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
