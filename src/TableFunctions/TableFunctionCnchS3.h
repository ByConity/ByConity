#pragma once
#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
class TableFunctionCnchS3 : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "CnchS3";
    std::string getName() const override
    {
        return name;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

private:
    StoragePtr getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "S3"; }
};

class TableFunctionCOS : public TableFunctionCnchS3
{
public:
    static constexpr auto name = "CnchCOS";
    std::string getName() const override
    {
        return name;
    }

private:
    const char * getStorageTypeName() const override { return "COSN"; }
};

}

#endif
