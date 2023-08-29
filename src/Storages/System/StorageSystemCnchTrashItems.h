#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;

class StorageSystemCnchTrashItems : public shared_ptr_helper<StorageSystemCnchTrashItems>,
                                    public IStorageSystemOneBlock<StorageSystemCnchTrashItems>
{
public:
    std::string getName() const override { return "SystemCnchTrashItems"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
