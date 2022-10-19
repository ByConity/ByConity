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
    std::vector<StatsTableIdentifier> tables;
    if (query->target_all)
    {
        if (!DatabaseCatalog::instance().isDatabaseExist(db))
        {
            auto msg = fmt::format(FMT_STRING("Unknown database ({})"), db);
            throw Exception(msg, ErrorCodes::UNKNOWN_DATABASE);
        }
        tables = catalog->getAllTablesID(db);
    }
    else
    {
        auto table_info_opt = catalog->getTableIdByName(db, query->table);
        if (!table_info_opt)
        {
            auto msg = "Unknown Table (" + query->table + ") in database (" + db + ")";
            throw Exception(msg, ErrorCodes::UNKNOWN_TABLE);
        }
        tables.push_back(table_info_opt.value());
    }

    for (auto & table : tables)
    {
        proxy->drop(table);
        catalog->invalidateClusterStatsCache(table);
    }

    return {};
}

}
