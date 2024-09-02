#pragma once
#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

std::optional<String> filterAndStripDatabaseNameIfTenanted(const String & tenant_id, const String & database_name);

/**
 * @brief Filter out all tables based on the selection query.
 *
 * @return A vector of tuples <database_fullname, database_name, table_name>.
 */
std::vector<std::tuple<String, String, String>> filterTables(const ContextPtr & context, const SelectQueryInfo & query_info);
}
