#pragma once
#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemWorkers: public shared_ptr_helper<StorageSystemWorkers>, public IStorageSystemOneBlock<StorageSystemWorkers>
{
public:
    std::string getName() const override { return "SystemWorkers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
