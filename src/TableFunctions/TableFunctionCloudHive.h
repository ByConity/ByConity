#pragma once

#include "Common/config.h"
#include "TableFunctions/ITableFunctionFileLike.h"
#if USE_HIVE

namespace DB
{

class TableFunctionCloudHive : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "cloudHive";

    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const override;

    const char * getStorageTypeName() const override { return "CloudHive"; }
};

}

#endif
