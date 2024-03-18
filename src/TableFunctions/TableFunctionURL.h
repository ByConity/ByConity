#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>


namespace DB
{
/* url(source, format, structure) - creates a temporary storage from url
 */
class TableFunctionURL : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "url";
    std::string getName() const override
    {
        return name;
    }

    bool isPreviledgedFunction() const override { return true; }

private:
    StoragePtr getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const override;
    const char * getStorageTypeName() const override { return "URL"; }
};
}
