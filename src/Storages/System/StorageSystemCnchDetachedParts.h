#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;

class StorageSystemCnchDetachedParts : public shared_ptr_helper<StorageSystemCnchDetachedParts>,
                                    public IStorageSystemOneBlock<StorageSystemCnchDetachedParts>
{
public:
    std::string getName() const override { return "SystemCnchDetachedParts"; }

    enum PartType
    {
        VisiblePart = 1,
        InvisiblePart = 2
    };

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
