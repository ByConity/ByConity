#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;

class StorageSystemCnchStagedParts : public shared_ptr_helper<StorageSystemCnchStagedParts>,
                                     public IStorageSystemOneBlock<StorageSystemCnchStagedParts>
{
public:

    StorageSystemCnchStagedParts(const StorageID & table_id_): IStorageSystemOneBlock(table_id_) {}

    std::string getName() const override { return "SystemCnchStagedParts"; }

    static NamesAndTypesList getNamesAndTypes();

    ColumnsDescription getColumnsAndAlias();

protected:
    const FormatSettings format_settings;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};
}
