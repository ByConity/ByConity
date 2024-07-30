#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `cnch_db_priv` system table to mimic mysql user table for user global privileges
class StorageSystemCnchDBPriv final : public shared_ptr_helper<StorageSystemCnchDBPriv>, public IStorageSystemOneBlock<StorageSystemCnchDBPriv>
{
public:
    std::string getName() const override { return "SystemCnchDBPriv"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemCnchDBPriv>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
