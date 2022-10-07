#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemCnchColumns : public shared_ptr_helper<StorageSystemCnchColumns>,
                                public IStorageSystemOneBlock<StorageSystemCnchColumns>
{
public:
    std::string getName() const override { return "SystemCnchColumns"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
