#include <memory>
#include <Interpreters/MySQL/InterpretersAnalyticalMySQLDDLQuery.h>

#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/MySQL/ASTAlterCommand.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <ResourceManagement/CommonData.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int UNKNOWN_TYPE;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int MYSQL_SYNTAX_ERROR;
}

namespace MySQLInterpreter
{

static NamesAndTypesList getColumnsList(const ASTExpressionList * columns_definition)
{
    NamesAndTypesList columns_name_and_type;
    for (const auto & declare_column_ast : columns_definition->children)
    {
        const auto & declare_column = declare_column_ast->as<ASTColumnDeclaration>();

        if (!declare_column || !declare_column->type)
            throw Exception("Missing type in definition of column.", ErrorCodes::UNKNOWN_TYPE);

        columns_name_and_type.emplace_back(declare_column->name, DataTypeFactory::instance().get(declare_column->type));

        ASTPtr data_type = declare_column->type;
        auto * data_type_function = data_type->as<ASTFunction>();

        if (data_type_function)
        {
            String type_name_upper = Poco::toUpper(data_type_function->name);

            if (type_name_upper.find("TIMESTAMP") != String::npos || type_name_upper.find("DATETIME") != String::npos)
            {
                if (type_name_upper.find("DATETIME64") == String::npos)
                {
                    data_type_function->name = "DATETIME64";
                    auto arguments = std::make_shared<ASTExpressionList>();
                    arguments->children.push_back(std::make_shared<ASTLiteral>(UInt8(DataTypeDateTime64::default_scale)));
                    if (data_type_function->arguments && !data_type_function->arguments->children.empty()) {
                        auto &args = data_type_function->arguments->children;
                        arguments->children.insert(arguments->children.end(), args.begin(), args.end());
                    }
                    if (type_name_upper.find("DateTimeWithoutTz") != String::npos)
                    {
                        arguments->children.push_back(std::make_shared<ASTLiteral>("UTC"));
                    }
                    data_type_function->arguments = arguments;
                }
            }
        }
    }

    return columns_name_and_type;
}

static void setNotNullModifier(ASTExpressionList * columns_definition, const NamesAndTypesList & primary_keys)
{
     for (const auto & primary_key : primary_keys)
    {
        for (auto & declare_column_ast : columns_definition->children)
        {
            auto declare_column = declare_column_ast->as<ASTColumnDeclaration>();
            if (declare_column->name == primary_key.name)
            {
               declare_column->null_modifier = false;
               if (!declare_column->default_specifier.empty() && declare_column->default_expression)
               {
                auto expr = declare_column->default_expression->as<ASTLiteral>();
                if (expr && expr->value.isNull())
                {
                    declare_column->default_expression = nullptr;
                    declare_column->default_specifier = "";
                }
               }
            }
        }

    }
}

static void convertDecimal(ASTExpressionList * columns_definition, const NamesAndTypesList & primary_keys)
{
     for (const auto & primary_key : primary_keys)
    {
        for (auto & declare_column_ast : columns_definition->children)
        {
            auto declare_column = declare_column_ast->as<ASTColumnDeclaration>();
            if (declare_column->name != primary_key.name) continue;
            ASTPtr data_type = declare_column->type;
            auto * data_type_function = data_type->as<ASTFunction>();

            if (!data_type_function) continue;

            String type_name_upper = Poco::toUpper(data_type_function->name);

            if (type_name_upper.find("DECIMAL") != String::npos && data_type_function->arguments->children.size() == 2)
            {
                data_type_function->name = "DECIMAL64";
                auto arguments = std::make_shared<ASTExpressionList>();
                arguments->children.push_back(data_type_function->arguments->children.back());
                data_type_function->arguments = arguments;
            }
        }

    }
}

static NamesAndTypesList getNames(const ASTFunction & expr, ContextPtr context, const NamesAndTypesList & columns)
{
    if (expr.arguments->children.empty())
        return NamesAndTypesList{};

    ASTPtr temp_ast = expr.clone();
    auto syntax = TreeRewriter(context).analyze(temp_ast, columns);
    auto required_columns = ExpressionAnalyzer(temp_ast, syntax, context).getActionsDAG(false)->getRequiredColumns();
    return required_columns;
}

static NamesAndTypesList modifyPrimaryKeysToNonNullable(const NamesAndTypesList & primary_keys, NamesAndTypesList & columns)
{
    /// https://dev.mysql.com/doc/refman/5.7/en/create-table.html#create-table-indexes-keys
    /// PRIMARY KEY:
    /// A unique index where all key columns must be defined as NOT NULL.
    /// If they are not explicitly declared as NOT NULL, MySQL declares them so implicitly (and silently).
    /// A table can have only one PRIMARY KEY. The name of a PRIMARY KEY is always PRIMARY,
    /// which thus cannot be used as the name for any other kind of index.
    NamesAndTypesList non_nullable_primary_keys;
    for (const auto & primary_key : primary_keys)
    {
        if (!primary_key.type->isNullable())
            non_nullable_primary_keys.emplace_back(primary_key);
        else
        {
            non_nullable_primary_keys.emplace_back(
                NameAndTypePair(primary_key.name, assert_cast<const DataTypeNullable *>(primary_key.type.get())->getNestedType()));

            for (auto & column : columns)
            {
                if (column.name == primary_key.name)
                    column.type = assert_cast<const DataTypeNullable *>(column.type.get())->getNestedType();
            }
        }
    }

    return non_nullable_primary_keys;
}

static std::tuple<NamesAndTypesList, NamesAndTypesList, NamesAndTypesList, NamesAndTypesList> getKeys(
    ASTExpressionList * columns_definition, ASTExpressionList * indices_define, ContextPtr context, NamesAndTypesList & columns)
{
    auto keys = makeASTFunction("tuple");
    auto unique_keys = makeASTFunction("tuple");
    auto primary_keys = makeASTFunction("tuple");
    auto cluster_keys = makeASTFunction("tuple");

    if (indices_define && !indices_define->children.empty())
    {
        NameSet columns_name_set;
        const Names & columns_name = columns.getNames();
        columns_name_set.insert(columns_name.begin(), columns_name.end());

        const auto & remove_prefix_key = [&](const ASTPtr & node) -> ASTPtr
        {
            auto res = std::make_shared<ASTExpressionList>();
            for (const auto & index_expression : node->children)
            {
                res->children.emplace_back(index_expression);

                if (const auto & function = index_expression->as<ASTFunction>())
                {
                    /// column_name(int64 literal)
                    if (columns_name_set.count(function->name) && function->arguments->children.size() == 1)
                    {
                        const auto & prefix_limit = function->arguments->children[0]->as<ASTLiteral>();

                        if (prefix_limit && isInt64OrUInt64FieldType(prefix_limit->value.getType()))
                            res->children.back() = std::make_shared<ASTIdentifier>(function->name);
                    }
                }
            }
            return res;
        };

        for (const auto & declare_index_ast : indices_define->children)
        {
            const auto & declare_index = declare_index_ast->as<MySQLParser::ASTDeclareIndex>();
            const auto & index_columns = remove_prefix_key(declare_index->index_columns);

            /// flatten
            if (startsWith(declare_index->index_type, "KEY_") || startsWith(declare_index->index_type, "INDEX_"))
            {
                if (context->getSettingsRef().exception_on_unsupported_mysql_syntax)
                {
                    throw Exception("MySQL index not supported yet", ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            else if (startsWith(declare_index->index_type, "UNIQUE_"))
                unique_keys->arguments->children.insert(unique_keys->arguments->children.end(),
                    index_columns->children.begin(), index_columns->children.end());
            if (startsWith(declare_index->index_type, "PRIMARY_KEY_"))
                primary_keys->arguments->children.insert(primary_keys->arguments->children.end(),
                    index_columns->children.begin(), index_columns->children.end());
            if (startsWith(declare_index->index_type, "CLUSTERED_KEY"))
                cluster_keys->arguments->children.insert(cluster_keys->arguments->children.end(),
                    index_columns->children.begin(), index_columns->children.end());
        }
    }

    for (const auto & column_ast : columns_definition->children)
    {
        const auto & column = column_ast->as<ASTColumnDeclaration>();
        if (column->mysql_primary_key)
            primary_keys->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(column->name));
        if (column->auto_increment && context->getSettingsRef().exception_on_unsupported_mysql_syntax)
        {
            throw Exception("auto_increment not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        }
        if (column->default_expression)
        {
            auto default_id = column->default_expression->as<ASTIdentifier>();
            if (default_id && Poco::toUpper(default_id->name()) == "CURRENT_TIMESTAMP")
            {
                column->default_expression = makeASTFunction("now64");
            }

            auto default_function = column->default_expression->as<ASTFunction>();
            if (default_function && Poco::toUpper(default_function->name) == "CURRENT_TIMESTAMP")
            {
                default_function->name = "now64";
            }
        }
    }

    const auto & primary_keys_names_and_types = getNames(*primary_keys, context, columns);
    const auto & non_nullable_primary_keys_names_and_types = modifyPrimaryKeysToNonNullable(primary_keys_names_and_types, columns);

    return std::make_tuple(non_nullable_primary_keys_names_and_types, getNames(*unique_keys, context, columns), getNames(*keys, context, columns), getNames(*cluster_keys, context, columns));
}

static ASTPtr getOrderByPolicy(
    const NamesAndTypesList & primary_keys,  const NamesAndTypesList & keys = NamesAndTypesList(), const NamesAndTypesList & cluster_keys = NamesAndTypesList())
{
    NameSet order_by_columns_set;
    std::deque<NamesAndTypesList> order_by_columns_list;

    const auto & add_order_by_expression = [&](const NamesAndTypesList & names_and_types)
    {
        NamesAndTypesList non_increment_keys;

        for (const auto & [name, type] : names_and_types)
        {
            if (order_by_columns_set.count(name))
                continue;

            order_by_columns_set.emplace(name);
            non_increment_keys.emplace_back(NameAndTypePair(name, type));
        }

        order_by_columns_list.emplace_front(non_increment_keys);
    };

    /// primary_key[not increment], key[not increment], unique[not increment]
    add_order_by_expression(cluster_keys);
    add_order_by_expression(keys);
    add_order_by_expression(primary_keys);

    auto order_by_expression = std::make_shared<ASTFunction>();
    order_by_expression->name = "tuple";
    order_by_expression->arguments = std::make_shared<ASTExpressionList>();

    for (const auto & order_by_columns : order_by_columns_list)
    {
        for (const auto & [name, type] : order_by_columns)
        {
            order_by_expression->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(name));

            if (type->isNullable())
                order_by_expression->arguments->children.back() = makeASTFunction("assumeNotNull", order_by_expression->arguments->children.back());
        }
    }

    return order_by_expression;
}

namespace
{

inline bool checkToInteger(DataTypePtr from, DataTypePtr to)
{
    return isInteger(from) && from->getSizeOfValueInMemory() <= to->getSizeOfValueInMemory();
}

inline bool checkToFloat(DataTypePtr from, DataTypePtr to)
{
    return isInteger(from) || (isFloat(from) && from->getSizeOfValueInMemory() <= to->getSizeOfValueInMemory());
}

inline bool checkToDecimal(DataTypePtr /*from*/, DataTypePtr /*to*/)
{
    /// TODO support change decimal type from low precision to high precision
    return false;
}

inline NamesAndTypesList getPrimaryKeys(const StorageInMemoryMetadata & metadata)
{
    ExpressionActionsPtr expr =  metadata.primary_key.expression;
    if (!expr)
        expr = metadata.sorting_key.expression;
    if (expr)
        return expr->getRequiredColumnsWithTypes();
    else
        return {};
}

void validateAlterColumnType(const AlterCommand & command, const StorageInMemoryMetadata & metadata)
{
    const auto & column_name = command.column_name;
    const auto & column = metadata.columns.get(column_name);
    auto from_type = column.type;
    auto to_type = command.data_type;

    if (from_type->equals(*to_type))
        return;

    if (from_type->isNullable() && !to_type->isNullable())
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Altering column type from NULL to NOT NULL is prohibitied.");

    auto nested_from_type = removeLowCardinalityAndNullable(from_type);
    auto nested_to_type = removeLowCardinalityAndNullable(to_type);

    if (!(nested_from_type->equals(*nested_to_type)
            || (isInteger(nested_to_type) && checkToInteger(nested_from_type, nested_to_type))
            || (isFloat(nested_to_type) && checkToFloat(nested_from_type, nested_to_type))
            || (isDecimal(nested_to_type) && checkToDecimal(nested_from_type, nested_to_type))))
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Altering column type is only allowed for the following cases:"\
                " narrow integer to wide integer or float/double or decimal; float to double");

}

void validateTTLExpression(const ASTPtr expression)
{
    const auto * ttl = expression->as<ASTExpressionList>();
    if (!ttl)
        return;

    for (const auto & child : ttl->children)
    {
        const auto * elem = child->as<ASTTTLElement>();
        if (!elem)
            continue;

        const auto * func = elem->ttl()->as<ASTFunction>();
        if (!func || !func->arguments || func->arguments->children.size() != 2)
            continue;

        if (func->name != "plus" && func->name != "minus")
            continue;

        const auto * to_date = func->arguments->children[0]->as<ASTFunction>();
        const auto * number_literal = func->arguments->children[1]->as<ASTLiteral>();

        if (!to_date || !number_literal)
            continue;

        if (to_date->name != "toDate" && to_date->name != "toDateTime")
            continue;

        throw Exception("TTL expression must not include toDate()/toDateTime() +/- numeric literal," \
        " please use +/- INTERVAL xxx DAY or proper functions instead. eg: addDate", ErrorCodes::MYSQL_SYNTAX_ERROR);
    }
}

}

void InterpreterAlterAnalyticalMySQLImpl::validate(const TQuery & query, ContextPtr context)
{
    for (const auto & ast : query.command_list->getChildren())
    {
        auto * command_ast = ast->as<ASTAlterCommand>();
        if (command_ast && (command_ast->type == ASTAlterCommand::ADD_COLUMN
                    || command_ast->type == ASTAlterCommand::DROP_COLUMN
                    || command_ast->type == ASTAlterCommand::MODIFY_COLUMN
                    || command_ast->type == ASTAlterCommand::RENAME_COLUMN
                    || command_ast->type == ASTAlterCommand::RENAME_TABLE
                    || command_ast->type == ASTAlterCommand::DROP_PARTITION))
        {
            auto table_id = context->resolveStorageID(query, Context::ResolveOrdinary);
            DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name, context);
            StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
            StorageInMemoryMetadata metadata = table->getInMemoryMetadata();

            if (auto command = AlterCommand::parse(command_ast, context))
            {
                if (command->type == AlterCommand::MODIFY_COLUMN)
                    validateAlterColumnType(*command, metadata);

                /// check changes to primary key
                if (command->type == AlterCommand::DROP_COLUMN || command->type == AlterCommand::MODIFY_COLUMN)
                {
                    const auto primary_col_names = getPrimaryKeys(metadata);
                    if (primary_col_names.contains(command->column_name))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot modify primary key columns.");
                }
                if (command->mysql_primary_key)
                    throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot add primary key.");
            }
        }
        else if (command_ast && (command_ast->type == ASTAlterCommand::ADD_INDEX || command_ast->type == ASTAlterCommand::DROP_INDEX))
        {
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "MySQL dialect doesn't supports alter query for ADD INDEX and DROP INDEX.");
        }
    }
}

ASTPtr InterpreterAlterAnalyticalMySQLImpl::getRewrittenQuery(const TQuery & query, ContextPtr context)
{
    auto new_query = std::make_shared<ASTAlterQuery>();

    for (const auto & ast : query.command_list->getChildren())
    {
        auto * command = ast->as<ASTAlterCommand>();
        if (command && command->type == ASTAlterCommand::RENAME_TABLE)
        {
            auto replace_query = std::make_shared<ASTRenameQuery>();
            bool need_database = context->getCurrentDatabase().empty() || context->getCurrentDatabase() == "default";
            auto to_database = need_database ? command->rename_table_to->as<ASTTableIdentifier>()->getDatabaseName() : "";
            auto to_table = command->rename_table_to->as<ASTTableIdentifier>()->getTableName();
            replace_query->elements.push_back({{query.database, query.table}, {to_database, to_table}});
            return replace_query;
        }
        if (command && command->type == ASTAlterCommand::NO_TYPE)
        {
            return {};
        }
    }
    *new_query = query;
    return new_query;
}


void InterpreterCreateAnalyticMySQLImpl::validate(const InterpreterCreateAnalyticMySQLImpl::TQuery & create_query, ContextPtr)
{
    if (const auto & mysql_storage = create_query.storage->as<ASTStorageAnalyticalMySQL>())
    {
        if (mysql_storage->life_cycle)
            throw Exception("LIFE CYCLE is not supported yet, please use TTL as an alternative", ErrorCodes::MYSQL_SYNTAX_ERROR);
        if (mysql_storage->ttl_table)
        {
            /// Throw exception for expression like toDate()/toDateTime +/- number
            validateTTLExpression(mysql_storage->ttl_table->ptr());
        }

        if (mysql_storage->engine)
        {
            auto upper_name = Poco::toUpper(mysql_storage->engine->name);
            if (!startsWith(upper_name, "CNCHMERGETREE"))
                return;
        }
    }

    auto * const create_defines = create_query.columns_list ? create_query.columns_list->as<ASTColumns>() : nullptr;

    if (!create_defines || !create_defines->columns || create_defines->columns->children.empty())
    {
        if (!create_query.as_table.empty() || create_query.as_table_function || create_query.select)
            return;

        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    }
}

ASTPtr InterpreterCreateAnalyticMySQLImpl::getRewrittenQuery( const TQuery & create_query, ContextPtr context)
{
    auto query = create_query.transform();
    auto rewritten_query = query->as<ASTCreateQuery>();

    const auto & create_defines = create_query.columns_list ? create_query.columns_list->as<ASTColumns>() : nullptr;
    const auto & mysql_storage = create_query.storage->as<ASTStorageAnalyticalMySQL>();
    auto & storage = rewritten_query->storage;
    bool has_table_definition = create_defines && create_defines->columns && !create_defines->columns->children.empty();
    bool create_as = !create_query.as_table.empty() || create_query.as_table_function;
    const std::unordered_set<String> engine_names
        = {"XUANWU",
           "PERFORMANCE_SCHEMA",
           "INNODB",
           "BLACKHOLE",
           "MYISAM",
           "MRG_MYISAM",
           "CSV",
           "ARCHIVE",
           "NDB",
           "MERGE",
           "FEDERATED",
           "EXAMPLE",
           "MEMORY",
           "HEAP",
           "OSS"};

    // ck definitions, just return raw ast
    String engine_name = "";
    if (storage->engine)
    {
        engine_name = Poco::toUpper(storage->engine->name);
        if (engine_names.find(engine_name) == engine_names.end())
        {
            if (mysql_storage->mysql_partition_by)
                storage->set(storage->partition_by, mysql_storage->mysql_partition_by->clone());
            return query;
        }
    }
    else if (mysql_storage->mysql_engine)
    {
        engine_name = Poco::toUpper(mysql_storage->mysql_engine->as<ASTLiteral>()->value.get<String>());
        if (engine_names.find(engine_name) == engine_names.end())
        {
            throw Exception ("Unsupported Engine Name", ErrorCodes::MYSQL_SYNTAX_ERROR);
        }
    }

    if ((create_as || rewritten_query->temporary) && (!has_table_definition && !create_query.select))
    {
        rewritten_query->storage = nullptr;
        return query;
    }

    // table
    if (has_table_definition && create_defines)
    {
        NamesAndTypesList columns_name_and_type = getColumnsList(create_defines->columns);
        const auto & [primary_keys, unique_keys, keys, cluster_keys] = getKeys(create_defines->columns, create_defines->mysql_indices, context, columns_name_and_type);

        setNotNullModifier(create_defines->columns, primary_keys);
        convertDecimal(create_defines->columns, primary_keys);

        if (ASTPtr order_by_expression = getOrderByPolicy(primary_keys, keys, cluster_keys))
        {
            auto & list = order_by_expression->as<ASTFunction>()->arguments;
            if (!list->children.empty())
            {
                storage->set(storage->order_by, order_by_expression);
            }
        }

        if (ASTPtr unique_key_expression = getOrderByPolicy(primary_keys))
        {
            storage->set(storage->unique_key, unique_key_expression);
        }

        rewritten_query->set(rewritten_query->columns_list, create_query.columns_list->clone());
        rewritten_query->columns_list->mysql_indices = nullptr;
    }

    // storage
    {
        if (!storage->engine || engine_names.find(Poco::toUpper(storage->engine->name)) != engine_names.end())
            storage->set(storage->engine, makeASTFunction("CnchMergeTree"));

        if (!storage->order_by)
            storage->set(storage->order_by, makeASTFunction("tuple"));

        if (!storage->unique_key)
        {
            // clickhouse syntax for primary key
            if (storage->primary_key)
            {
                storage->set(storage->unique_key, storage->primary_key->clone());
                storage->primary_key = nullptr;
            }
            else
            {
                storage->set(storage->unique_key, makeASTFunction("tuple"));
            }
        }

        if (mysql_storage->mysql_partition_by)
        {
            storage->set(storage->partition_by, mysql_storage->mysql_partition_by->clone());
        }

        // settings
        ASTPtr settings = std::make_shared<ASTSetQuery>();
        auto *settings_ast = settings->as<ASTSetQuery>();
        settings_ast->is_standalone = false;
        bool has_index_granularity_setting = false;
        bool has_partition_level_unique_keys_setting = false;
        bool has_enable_bucket_level_unique_keys = false;
        bool has_enable_bucket_for_distribute = context->getSettingsRef().enable_bucket_for_distribute;
        auto *const mysql_settings = mysql_storage->settings ? mysql_storage->settings->as<ASTSetQuery>() : nullptr;
        if (mysql_settings)
        {
            for (const auto & change: mysql_settings->changes)
            {
                if (change.name == "index_granularity")
                    has_index_granularity_setting = true;
                if (change.name == "partition_level_unique_keys")
                    has_partition_level_unique_keys_setting = true;
                if (change.name == "enable_bucket_level_unique_keys")
                    has_enable_bucket_level_unique_keys = true;
            }
        }

        // block_size -> index_granularity
        if (mysql_storage->block_size && !has_index_granularity_setting)
            settings_ast->changes.push_back({"index_granularity", mysql_storage->block_size->as<ASTLiteral>()->value.get<Int64>()});

        // distributed by hash(col) -> cluster by col
        if (mysql_storage->distributed_by && has_enable_bucket_for_distribute)
        {
            const String vw_name = "vw_default";
            auto vw = context->getVirtualWarehousePool().get(vw_name);

            int total_bucket_number = vw ? vw->getNumWorkers() : 1;
            auto cluster_by_ast = std::make_shared<ASTClusterByElement>(mysql_storage->distributed_by->clone(), std::make_shared<ASTLiteral>(total_bucket_number), 0, false, false);
            storage->set(storage->cluster_by, cluster_by_ast);

            // distribute by must contain unique key
            if (!has_enable_bucket_level_unique_keys)
                settings_ast->changes.push_back({"enable_bucket_level_unique_keys", 1});
        }
        else if (mysql_storage->cluster_by)
        {
            // clickhouse cluster by syntax
            storage->set(storage->cluster_by, mysql_storage->cluster_by->clone());
        }

        // storage settings for mysql behavior
        if (!has_partition_level_unique_keys_setting)
            settings_ast->changes.push_back({"partition_level_unique_keys", 0});

        if (mysql_settings)
            settings_ast->changes.insert(settings_ast->changes.end(), mysql_settings->changes.begin(), mysql_settings->changes.end());

        storage->set(storage->settings, settings);
    }

    return query;
}

}

}
