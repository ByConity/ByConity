#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;


/** Implements `cnch_async_queries` system table, which allows you to get information about the queries that are asynchronously executing.
  */
class StorageSystemCnchAsyncQueries final : public shared_ptr_helper<StorageSystemCnchAsyncQueries>,
                                            public IStorageSystemOneBlock<StorageSystemCnchAsyncQueries>
{
    friend struct shared_ptr_helper<StorageSystemCnchAsyncQueries>;

public:
    std::string getName() const override
    {
        return "SystemCnchAsyncQueries";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
