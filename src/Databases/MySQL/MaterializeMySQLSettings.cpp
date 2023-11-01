#include <Databases/MySQL/MaterializeMySQLSettings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(MaterializeMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)

void MaterializeMySQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            auto pre_include_tables = include_tables;
            auto pre_exclude_tables = exclude_tables;
            applyChanges(storage_def.settings->changes);
            if (!include_tables.value.empty() && !exclude_tables.value.empty())
            {
                include_tables = pre_include_tables;
                exclude_tables = pre_exclude_tables;
                throw Exception("Can not set both settings: include_tables and exclude_tables", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for database " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

}
