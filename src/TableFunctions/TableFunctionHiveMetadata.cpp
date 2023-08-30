#include "TableFunctions/TableFunctionHiveMetadata.h"
#if USE_HIVE

#include "Columns/IColumn.h"
#include "DataTypes/DataTypeString.h"
#include "Interpreters/evaluateConstantExpression.h"
#include "Parsers/ASTLiteral.h"
#include "Storages/Hive/Metastore/HiveMetastore.h"
#include "Storages/StorageValues.h"
#include "TableFunctions/TableFunctionFactory.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

static NamesAndTypesList getHeaderForHiveMetadata()
{
    NamesAndTypesList names_and_types {
        {"descriptions", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}

void TableFunctionHiveMetadata::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    ASTs & args = args_func.at(0)->children;

    if (args.size() != 3)
        throw Exception("Table function '" + getName() + "' requires exact three argument: "
                        " hive_metasture_url, hive_database_name, hive_table_name",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    hive_metastore_url = args[0]->as<const ASTLiteral &>().value.safeGet<String>();
    hive_database_name = args[1]->as<const ASTLiteral &>().value.safeGet<String>();
    hive_table_name = args[2]->as<const ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionHiveMetadata::getActualTableStructure(ContextPtr) const
{
    return ColumnsDescription(getHeaderForHiveMetadata());
}

void TableFunctionHiveMetadata::fillData(MutableColumns & columns) const
{
    auto hive_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url);
    auto hive_table = hive_client->getTable(hive_database_name, hive_table_name);

    std::stringstream ss;
    hive_table->printTo(ss);
    columns[0]->insert(ss.str());
}

StoragePtr TableFunctionHiveMetadata::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto names_and_types = getHeaderForHiveMetadata();
    Block sample_block;
    for (const auto & column : names_and_types)
        sample_block.insert({column.type->createColumn(), column.type, column.name});

    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    fillData(res_columns);
    sample_block.setColumns(std::move(res_columns));

    return StorageValues::create(StorageID(getDatabaseName(), table_name), columns, sample_block);
}

void registerTableFunctionHiveMetadata(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHiveMetadata>();
}

}
#endif
