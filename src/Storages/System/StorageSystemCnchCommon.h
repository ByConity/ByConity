#pragma once
#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/**
 * @brief Filter out all tables based on the selection query.
 *
 * @return A vector of pair <database_name, table_name>.
 */
std::vector<std::pair<String, String>> filterTables(const ContextPtr & context, const SelectQueryInfo & query_info);
}
