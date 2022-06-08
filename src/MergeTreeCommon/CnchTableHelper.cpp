#include <MergeTreeCommon/CnchTableHelper.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>

namespace DB::CnchTableHelper
{
/// Create storage object without database
StoragePtr createStorageFromQuery(const std::string & create_table_query, Context & context)
{
    ParserCreateQuery p_create_query;
    auto ast = parseQuery(p_create_query, create_table_query, context.getSettingsRef().max_query_size
        , context.getSettingsRef().max_parser_depth);
    auto & create_query = ast->as<ASTCreateQuery &>();

    // StorageSelector storage_selector("store", true);

    return StorageFactory::instance().get(
        create_query,
        "",
        context.getQueryContext(),
        context.getGlobalContext(),
        InterpreterCreateQuery::getColumnsDescription(*create_query.columns_list->columns, context.getSessionContext(), false),
        InterpreterCreateQuery::getConstraintsDescription(create_query.columns_list->constraints),
        false /*has_force_restore_data_flag*/);
}

}
