#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `sensitive grants` system table, which allows you to get information about sensitive grants.
class StorageSystemSensitiveGrants final : public shared_ptr_helper<StorageSystemSensitiveGrants>, public IStorageSystemOneBlock<StorageSystemSensitiveGrants>
{
public:
    std::string getName() const override { return "SystemSensitiveGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemSensitiveGrants>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
