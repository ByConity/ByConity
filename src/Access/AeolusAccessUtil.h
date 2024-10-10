#pragma once

#include <Parsers/parseDatabaseAndTableName.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

namespace DB
{

bool aeolusCheck(const Context & context, const String & full_table_name)
{
    String access_table_names = context.getSettingsRef().access_table_names;

    if (access_table_names.empty())
        access_table_names = context.getSettingsRef().accessible_table_names;

    if (access_table_names.empty())
        return true;

    std::unordered_set<String> allowed_tables;
    std::vector<String> tables;
    boost::split(tables, access_table_names, boost::is_any_of(" ,"));
    allowed_tables.insert(tables.begin(), tables.end());

    for (auto & table : tables)
    {
        char * begin = table.data();
        char * end = begin + table.size();
        Tokens tokens(begin, end);
        IParser::Pos token_iterator(tokens, context.getSettingsRef().max_parser_depth);
        auto pos = token_iterator;
        Expected expected;
        String database_name, table_name;
        if (!parseDatabaseAndTableName(pos, expected, database_name, table_name))
            continue;

        StorageID table_id{database_name, table_name};
        if (auto tenant_id = getCurrentTenantId(); !tenant_id.empty() && !table_id.database_name.empty())
        {
            table_id.database_name = tenant_id + '.' + database_name;
            allowed_tables.emplace(table_id.getFullNameNotQuoted());
        }

        /// tryGetTable below requires resolved table id
        StorageID resolved = context.tryResolveStorageID(table_id);
        if (!resolved)
            continue;

        /// access_table_names need to have resolved name, otherwise tryGetTable below will fail
        if (table_id.database_name.empty() && !resolved.database_name.empty())
            allowed_tables.emplace(formatTenantDatabaseName(resolved.getDatabaseName()) + "." + resolved.getTableName());
    }

    return allowed_tables.count(full_table_name) > 0;
}

}
