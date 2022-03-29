#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** `system`.`metastore` helps to investigate metadata stored in metastore. In current implementation, each storage has its own metastore,
 *  so that the database and table should be specified when querying this table.
*/
class StorageSystemMetastore final : public shared_ptr_helper<StorageSystemMetastore>, public IStorageSystemOneBlock<StorageSystemMetastore>
{
    friend struct shared_ptr_helper<StorageSystemMetastore>;
public:
    std::string getName() const override { return "SystemMetastore"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
