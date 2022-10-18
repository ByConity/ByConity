#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropStatsQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Statistics/CachedStatsProxy.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsTableBasic.h>
namespace DB
{
using namespace Statistics;
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterDropStatsQuery::execute()
{
    auto context = getContext();
    auto query = query_ptr->as<const ASTDropStatsQuery>();
    auto catalog = Statistics::createCatalogAdaptor(context);

    catalog->checkHealth(/*is_write=*/true);

    auto proxy = Statistics::createCachedStatsProxy(catalog);
    auto db = context->resolveDatabase(query->database);
    if (query->target_all)
    {
        std::vector<StatsTableIdentifier> tables;
        if (!DatabaseCatalog::instance().isDatabaseExist(db))
        {
            auto msg = fmt::format(FMT_STRING("Unknown database ({})"), db);
            throw Exception(msg, ErrorCodes::UNKNOWN_DATABASE);
        }
        tables = catalog->getAllTablesID(db);
        for (auto & table : tables)
        {
            proxy->drop(table);
            catalog->invalidateClusterStatsCache(table);
        }
    }
    else
    {
        auto table_info_opt = catalog->getTableIdByName(db, query->table);
        if (!table_info_opt)
        {
            auto msg = "Unknown Table (" + query->table + ") in database (" + db + ")";
            throw Exception(msg, ErrorCodes::UNKNOWN_TABLE);
        }
        auto table = table_info_opt.value();

        if (!query->columns.empty())
        {
            auto cols_desc = filterCollectableColumns(catalog->getCollectableColumns(table), query->columns, true);
            proxy->dropColumns(table, cols_desc);
            catalog->invalidateClusterStatsCache(table);
        }
        else
        {
            proxy->drop(table);
            catalog->invalidateClusterStatsCache(table);
        }
    }


    return {};
}

}
