#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;

class StorageSystemBitmapIndex final : public shared_ptr_helper<StorageSystemBitmapIndex>, public IStorageSystemOneBlock<StorageSystemBitmapIndex>
{
    friend struct shared_ptr_helper<StorageSystemBitmapIndex>;
public:
    std::string getName() const override { return "SystemBitmapIndex"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
