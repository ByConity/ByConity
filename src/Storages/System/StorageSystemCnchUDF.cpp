#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Storages/System/StorageSystemCnchUDF.h>
#include <Storages/System/CollectWhereClausePredicate.h>

namespace DB
{
NamesAndTypesList StorageSystemCnchUDF::getNamesAndTypes()
{
    return {
        {"database_name", std::make_shared<DataTypeString>()},
        {"function_name", std::make_shared<DataTypeString>()},
        {"function_core", std::make_shared<DataTypeString>()},
        {"version", std::make_shared<DataTypeUInt64>()},
        {"or_replace", std::make_shared<DataTypeUInt8>()},
        {"if_not_exists", std::make_shared<DataTypeUInt8>()},
        {"is_lambda", std::make_shared<DataTypeUInt8>()}
    };
}

void StorageSystemCnchUDF::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    auto cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_user_defined_functions only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    std::map<String, String> columnToValue;
    String database_name, function_name;

    auto database_it = columnToValue.find("database");
    if (database_it != columnToValue.end())
    {
        database_name = database_it->second;
    }

    auto function_it = columnToValue.find("name");
    if (function_it != columnToValue.end())
    {
        function_name = function_it->second;
    }

    auto it = cnch_catalog->getAllUDFs(database_name, function_name);

    for (size_t i = 0, size = it.size(); i != size; ++i)
    {
        size_t col_num = 0;
        auto udfModel = it[i];
        auto object_create_query = udfModel.function_definition();
        ParserCreateFunctionQuery parser;
        ASTPtr astptr = parseQuery(
            parser,
            object_create_query.data(),
            object_create_query.data() + object_create_query.size(),
            "not in file ",
            0,
            context->getSettingsRef().max_parser_depth);
        const auto & ast = astptr->as<ASTCreateFunctionQuery>();

        res_columns[col_num++]->insert(ast->database_name);
        res_columns[col_num++]->insert(ast->function_name);
        
        WriteBufferFromOwnString buf;
        formatAST(*(ast->function_core), buf, false);
        String body = buf.str();
        res_columns[col_num++]->insert(body);

        res_columns[col_num++]->insert(ast->version);
        res_columns[col_num++]->insert(ast->or_replace);
        res_columns[col_num++]->insert(ast->if_not_exists);
        res_columns[col_num++]->insert(ast->is_lambda);

    }
}

}
