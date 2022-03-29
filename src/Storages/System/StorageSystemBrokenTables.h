#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** System table "broken_tables" which record those table cannot be loaded during instance startup
  */
class StorageSystemBrokenTables final : public shared_ptr_helper<StorageSystemBrokenTables>, public IStorageSystemOneBlock<StorageSystemBrokenTables>
{
public:
    std::string getName() const override { return "SystemBrokenTables"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
