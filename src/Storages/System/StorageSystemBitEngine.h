#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemBitEngine final : public shared_ptr_helper<StorageSystemBitEngine>, public IStorageSystemOneBlock<StorageSystemBitEngine>
{
    friend struct shared_ptr_helper<StorageSystemBitEngine>;
public:
    std::string getName() const override { return "SystemBitEngine"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
