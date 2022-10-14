#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;

/***
 * In cnch, we may have multiple servers and each server hosts a number of tables. The system.cnch_table_host is used to get target server to which the query regarding to the tables should be
 * rooted.
 */
class StorageSystemCnchTableHost : public shared_ptr_helper<StorageSystemCnchTableHost>,
                                public IStorageSystemOneBlock<StorageSystemCnchTableHost>
{
public:
    std::string getName() const override { return "SystemCnchTableHost"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
