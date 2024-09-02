#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `cnch_user_priv` system table to mimic mysql user table for user global privileges
class StorageSystemCnchUserPriv final : public shared_ptr_helper<StorageSystemCnchUserPriv>, public IStorageSystemOneBlock<StorageSystemCnchUserPriv>
{
public:
    std::string getName() const override { return "SystemCnchUserPriv"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemCnchUserPriv>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
