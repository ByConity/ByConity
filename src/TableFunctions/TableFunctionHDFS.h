#pragma once
#include <Common/config.h>

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
class TableFunctionHDFS : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "CnchHDFS";

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    std::string getName() const override
    {
        return name;
    }

    bool isPreviledgedFunction() const override { return true; }

private:
    StoragePtr getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "HDFS"; }
};
}

#endif
