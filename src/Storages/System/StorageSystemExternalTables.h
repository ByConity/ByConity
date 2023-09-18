#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;

class StorageSystemExternalTables : public shared_ptr_helper<StorageSystemExternalTables>,
                                    public IStorageSystemOneBlock<StorageSystemExternalTables>
{
public:
    std::string getName() const override { return "StorageSystemExternalTables"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
