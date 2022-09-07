#include <Statistics/TableHandler.h>
#include <fmt/format.h>

namespace DB::Statistics
{

void TableHandler::registerHandler(std::unique_ptr<ColumnHandlerBase> handler)
{
    column_size += handler->size();
    handlers.emplace_back(std::move(handler));
}


String TableHandler::getFullSql()
{
    std::vector<String> sql_components;
    for (auto & handler : handlers)
    {
        auto sqls = handler->getSqls();
        sql_components.insert(sql_components.end(), sqls.begin(), sqls.end());
    }
    auto full_sql = fmt::format(FMT_STRING("select {} from {}"), fmt::join(sql_components, ", "), table_identifier.getDbTableName());
    LOG_INFO(&Logger::get("TableHandler"), "full_sql=" << full_sql);
    return full_sql;
}

void TableHandler::parse(const Block & block)
{
    LOG_INFO(&Logger::get("TableHandler"), "table=" << table_identifier.getDbTableName());
    if (block.columns() != column_size)
    {
        throw Exception("fetched block has wrong column size", ErrorCodes::LOGICAL_ERROR);
    }

    size_t column_offset = 0;
    for (auto & handler : handlers)
    {
        handler->parse(block, column_offset);
        column_offset += handler->size();
    }

    if (column_offset != column_size)
    {
        throw Exception("block column not match", ErrorCodes::LOGICAL_ERROR);
    }
}

}