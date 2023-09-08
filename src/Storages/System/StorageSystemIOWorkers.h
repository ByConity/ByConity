#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

class StorageSystemIOWorkers: public shared_ptr_helper<StorageSystemIOWorkers>, public IStorageSystemOneBlock<StorageSystemIOWorkers>
{
public:
    std::string getName() const override { return "SystemIOWorkers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
