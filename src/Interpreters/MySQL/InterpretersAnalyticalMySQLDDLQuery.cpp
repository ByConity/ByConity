#include <memory>
#include <Interpreters/MySQL/InterpretersAnalyticalMySQLDDLQuery.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTAlterCommand.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/ASTClusterByElement.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Common/quoteString.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTRenameQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <ResourceManagement/CommonData.h>
#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>


#if USE_MYSQL
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#endif

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
            /// Transforms MySQL ENUM's list of strings to ClickHouse string-integer pairs
            /// For example ENUM('a', 'b', 'c') -> ENUM('a'=1, 'b'=2, 'c'=3)
            /// Elements on a position further than 32767 are assigned negative values, starting with -32768.
            /// Note: Enum would be transformed to Enum8 if number of elements is less then 128, otherwise it would be transformed to Enum16.
            if (type_name_upper.find("ENUM") != String::npos)
            {
                UInt16 i = 0;
                for (ASTPtr & child : data_type_function->arguments->children)
                {
                    auto new_child = std::make_shared<ASTFunction>();
                    new_child->name = "equals";
                    auto * literal = child->as<ASTLiteral>();

                    if (!literal)
                        throw Exception("ENUM is not supported yet", ErrorCodes::MYSQL_SYNTAX_ERROR);


                    new_child->arguments = std::make_shared<ASTExpressionList>();
                    new_child->arguments->children.push_back(std::make_shared<ASTLiteral>(literal->value.get<String>()));
                    new_child->arguments->children.push_back(std::make_shared<ASTLiteral>(Int16(++i)));
                    child = new_child;
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
                declare_column->null_modifier = false;
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

static std::tuple<NamesAndTypesList, NamesAndTypesList, NamesAndTypesList, NameSet, NamesAndTypesList> getKeys(
    ASTExpressionList * columns_definition, ASTExpressionList * indices_define, ContextPtr context, NamesAndTypesList & columns)
{
    NameSet increment_columns;
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
                throw Exception("MySQL index not supported yet", ErrorCodes::NOT_IMPLEMENTED);
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
        if (column->auto_increment)
            throw Exception("auto_increment not supported yet", ErrorCodes::NOT_IMPLEMENTED);
    }

    const auto & primary_keys_names_and_types = getNames(*primary_keys, context, columns);
    const auto & non_nullable_primary_keys_names_and_types = modifyPrimaryKeysToNonNullable(primary_keys_names_and_types, columns);
    return std::make_tuple(non_nullable_primary_keys_names_and_types, getNames(*unique_keys, context, columns), getNames(*keys, context, columns), increment_columns, getNames(*cluster_keys, context, columns));
}

static ASTPtr getPartitionPolicy(const NamesAndTypesList & primary_keys)
{
    const auto & numbers_partition = [&](const String & column_name, size_t type_max_size) -> ASTPtr
    {
        if (type_max_size <= 1000)
            return std::make_shared<ASTIdentifier>(column_name);

        return makeASTFunction("intDiv", std::make_shared<ASTIdentifier>(column_name),
           std::make_shared<ASTLiteral>(UInt64(type_max_size / 1000)));
    };

    ASTPtr best_partition;
    size_t best_size = 0;
    for (const auto & primary_key : primary_keys)
    {
        DataTypePtr type = primary_key.type;
        WhichDataType which(type);

        if (which.isNullable())
            throw Exception("LOGICAL ERROR: MySQL primary key must be not null, it is a bug.", ErrorCodes::LOGICAL_ERROR);

        if (which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64())
        {
            /// In any case, date or datetime is always the best partitioning key
            return makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(primary_key.name));
        }

        if (type->haveMaximumSizeOfValue() && (!best_size || type->getSizeOfValueInMemory() < best_size))
        {
            if (which.isInt8() || which.isUInt8())
            {
                best_size = type->getSizeOfValueInMemory();
                best_partition = numbers_partition(primary_key.name, std::numeric_limits<UInt8>::max());
            }
            else if (which.isInt16() || which.isUInt16())
            {
                best_size = type->getSizeOfValueInMemory();
                best_partition = numbers_partition(primary_key.name, std::numeric_limits<UInt16>::max());
            }
            else if (which.isInt32() || which.isUInt32())
            {
                best_size = type->getSizeOfValueInMemory();
                best_partition = numbers_partition(primary_key.name, std::numeric_limits<UInt32>::max());
            }
            else if (which.isInt64() || which.isUInt64())
            {
                best_size = type->getSizeOfValueInMemory();
                best_partition = numbers_partition(primary_key.name, std::numeric_limits<UInt64>::max());
            }
        }
    }

    return best_partition;
}

static ASTPtr getOrderByPolicy(
    const NamesAndTypesList & primary_keys, const NamesAndTypesList & unique_keys, const NamesAndTypesList & keys, const NameSet & increment_columns, const NamesAndTypesList & cluster_keys = NamesAndTypesList())
{
    NameSet order_by_columns_set;
    std::deque<NamesAndTypesList> order_by_columns_list;

    const auto & add_order_by_expression = [&](const NamesAndTypesList & names_and_types)
    {
        NamesAndTypesList increment_keys;
        NamesAndTypesList non_increment_keys;

        for (const auto & [name, type] : names_and_types)
        {
            if (order_by_columns_set.count(name))
                continue;

            if (increment_columns.count(name))
            {
                order_by_columns_set.emplace(name);
                increment_keys.emplace_back(NameAndTypePair(name, type));
            }
            else
            {
                order_by_columns_set.emplace(name);
                non_increment_keys.emplace_back(NameAndTypePair(name, type));
            }
        }

        order_by_columns_list.emplace_back(increment_keys);
        order_by_columns_list.emplace_front(non_increment_keys);
    };

    /// primary_key[not increment], key[not increment], unique[not increment], unique[increment], key[increment], primary_key[increment]
    /// cluster keys in the begining
    add_order_by_expression(cluster_keys);
    add_order_by_expression(unique_keys);
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

}

void InterpreterAlterAnalyticalMySQLImpl::validate(const TQuery & query, ContextPtr context)
{
    if (query.alter_object != ASTAlterQuery::AlterObjectType::TABLE)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only support Alter Table under MySQL dialect. But you are altering other objects.");

    for (const auto & ast : query.command_list->getChildren())
    {
        auto * command = ast->as<ASTAlterCommand>();
        if (command && command->type != ASTAlterCommand::ADD_COLUMN
                    && command->type != ASTAlterCommand::DROP_COLUMN
                    && command->type != ASTAlterCommand::MODIFY_COLUMN
                    && command->type != ASTAlterCommand::RENAME_COLUMN
                    && command->type != ASTAlterCommand::RENAME_TABLE
                    && command->type != ASTAlterCommand::DROP_PARTITION)
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "MySQL dialect only supports alter query for ADD COLUMN, DROP Column, MODIFY COLUMN and RENAME COLUMN.");
    }

    auto table_id = context->resolveStorageID(query, Context::ResolveOrdinary);
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name, context);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();

    for (const auto & ast : query.command_list->getChildren())
    {
        auto * command_ast = ast->as<ASTAlterCommand>();
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
}

ASTs InterpreterAlterAnalyticalMySQLImpl::getRewrittenQueries(const TQuery & query, ContextPtr context)
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
            return ASTs{replace_query};
        }
        if (command && command->type == ASTAlterCommand::NO_TYPE)
        {
            return {};
        }
    }
    *new_query = query;
    return ASTs{new_query};
}


void InterpreterCreateAnalyticMySQLImpl::validate(const InterpreterCreateAnalyticMySQLImpl::TQuery & create_query, ContextPtr)
{
    if (const auto & mysql_storage = create_query.storage->as<ASTStorageAnalyticalMySQL>())
    {
        if (mysql_storage->primary_key)
            throw Exception("This is Clickhouse syntax for PRIMARY KEY, please follow mysql dialect", ErrorCodes::MYSQL_SYNTAX_ERROR);
        if (mysql_storage->cluster_by)
            throw Exception("This is Clickhouse syntax for CLUSTER BY, please follow mysql dialect: DISTRIBUTED BY", ErrorCodes::MYSQL_SYNTAX_ERROR);
        if (mysql_storage->life_cycle)
            throw Exception("LIFE CYCLE is not supported yet, please use TTL as an alternative", ErrorCodes::MYSQL_SYNTAX_ERROR);

        if (mysql_storage->engine)
        {
            auto upper_name = Poco::toUpper(mysql_storage->engine->name);
            if (!startsWith(upper_name, "CNCHMERGETREE"))
                return;
        }
    }

    const auto & create_defines = create_query.columns_list->as<ASTColumns>();

    if (!create_defines || !create_defines->columns || create_defines->columns->children.empty())
    {
        if (!create_query.as_table.empty() || create_query.as_table_function || create_query.select)
            return;

        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    }
}

ASTs InterpreterCreateAnalyticMySQLImpl::getRewrittenQueries( const TQuery & create_query, ContextPtr context)
{
    auto query = create_query.transform();
    auto rewritten_query = query->as<ASTCreateQuery>();

    const auto & create_defines = create_query.columns_list->as<ASTColumns>();
    const auto & mysql_storage = create_query.storage->as<ASTStorageAnalyticalMySQL>();
    auto & storage = rewritten_query->storage;
    bool has_table_definition = create_defines && create_defines->columns && !create_defines->columns->children.empty();
    bool create_as = !create_query.as_table.empty() || create_query.as_table_function;
    bool has_unique_key = false;

    // ck definitions, just return raw ast
    if (storage->engine)
    {
        auto upper_name = Poco::toUpper(storage->engine->name);
        if (!startsWith(upper_name, "XUANWU"))
        {
            if (mysql_storage->mysql_partition_by)
                storage->set(storage->partition_by, mysql_storage->mysql_partition_by->clone());
            return ASTs{query};
        }
    }

    if (create_as || rewritten_query->temporary)
    {
        rewritten_query->storage = nullptr;
        return ASTs{query};
    }

    if (has_table_definition)
    {
        NamesAndTypesList columns_name_and_type = getColumnsList(create_defines->columns);
        const auto & [primary_keys, unique_keys, keys, increment_columns, cluster_keys] = getKeys(create_defines->columns, create_defines->mysql_indices, context, columns_name_and_type);

        setNotNullModifier(create_defines->columns, primary_keys);

        /// The `partition by` expression must use primary keys, otherwise the primary keys will not be merge.
        if (ASTPtr partition_expression = getPartitionPolicy(primary_keys))
            storage->set(storage->partition_by, partition_expression);
        if (mysql_storage->mysql_partition_by)
            storage->set(storage->partition_by, mysql_storage->mysql_partition_by->clone());

        /// The `order by` expression must use primary keys, otherwise the primary keys will not be merge.
        if (ASTPtr order_by_expression = getOrderByPolicy(primary_keys, unique_keys, keys, increment_columns, cluster_keys))
        {
            auto & list = order_by_expression->as<ASTFunction>()->arguments;
            if (!list->children.empty())
            {
                storage->set(storage->order_by, order_by_expression);
            }
        }

        if (ASTPtr unique_key_expression = getOrderByPolicy(primary_keys, unique_keys, keys, increment_columns))
        {
            storage->set(storage->unique_key, unique_key_expression);
            has_unique_key = true;
        }

        rewritten_query->set(rewritten_query->columns_list, create_query.columns_list->clone());
        rewritten_query->columns_list->mysql_indices = nullptr;
    }

    if (!storage->engine)
        storage->set(storage->engine, makeASTFunction("CnchMergeTree"));
    if (!storage->order_by)
        storage->set(storage->order_by, makeASTFunction("tuple"));

    {
        ASTPtr settings = std::make_shared<ASTSetQuery>();
        auto settings_ast = settings->as<ASTSetQuery>();
        settings_ast->is_standalone = false;
        // It's not recommended to mix mysql and clickhosue dialects
        // but we have to provide this in case of fall back
        if (mysql_storage->block_size)
        {
            // block_size -> index_granularity
            settings_ast->changes.push_back({"index_granularity", mysql_storage->block_size->as<ASTLiteral>()->value.get<Int64>()});
        }

        if (mysql_storage->distributed_by)
        {
            // distributed by hash(col) -> cluster by col
            const String vw_name = "vw_default";
            auto vw = context->getVirtualWarehousePool().get(vw_name);
            // context->setCurrentVW(std::move(vw_handle));
            // auto vw = context->tryGetCurrentVW();
            int total_bucket_number = vw ? vw->getNumWorkers() : 1;
            auto cluster_by_ast = std::make_shared<ASTClusterByElement>(mysql_storage->distributed_by->clone(), std::make_shared<ASTLiteral>(total_bucket_number), 0, false, false);
            storage->set(storage->cluster_by, cluster_by_ast);
        }

        if (has_unique_key)
            settings_ast->changes.push_back({"partition_level_unique_keys", 0});
        if (const auto mysql_settings = mysql_storage->settings->as<ASTSetQuery>())
            settings_ast->changes.insert(settings_ast->changes.end(), mysql_settings->changes.begin(), mysql_settings->changes.end());

        storage->set(storage->settings, settings);
    }

    return ASTs{query};
}

// void InterpreterDropImpl::validate(const InterpreterDropImpl::TQuery & /*query*/, ContextPtr /*context*/)
// {
// }

// ASTs InterpreterDropImpl::getRewrittenQueries(
//     const InterpreterDropImpl::TQuery & drop_query, ContextPtr context, const String & mapped_to_database, const String & mysql_database)
// {
//     const auto & database_name = resolveDatabase(drop_query.database, mysql_database, mapped_to_database, context);

//     /// Skip drop database|view|dictionary
//     if (database_name != mapped_to_database || drop_query.table.empty() || drop_query.is_view || drop_query.is_dictionary)
//         return {};

//     ASTPtr rewritten_query = drop_query.clone();
//     rewritten_query->as<ASTDropQuery>()->database = mapped_to_database;
//     rewritten_query->as<ASTDropQuery>()->if_exists = true;
//     return ASTs{rewritten_query};
// }

// void InterpreterRenameImpl::validate(const InterpreterRenameImpl::TQuery & rename_query, ContextPtr /*context*/)
// {
//     if (rename_query.exchange)
//         throw Exception("Cannot execute exchange for external ddl query.", ErrorCodes::NOT_IMPLEMENTED);
// }

// ASTs InterpreterRenameImpl::getRewrittenQueries(
//     const InterpreterRenameImpl::TQuery & rename_query, ContextPtr context, const String & mapped_to_database, const String & mysql_database)
// {
//     ASTRenameQuery::Elements elements;
//     for (const auto & rename_element : rename_query.elements)
//     {
//         const auto & to_database = resolveDatabase(rename_element.to.database, mysql_database, mapped_to_database, context);
//         const auto & from_database = resolveDatabase(rename_element.from.database, mysql_database, mapped_to_database, context);

//         if ((from_database == mapped_to_database || to_database == mapped_to_database) && to_database != from_database)
//             throw Exception("Cannot rename with other database for external ddl query.", ErrorCodes::NOT_IMPLEMENTED);

//         if (from_database == mapped_to_database)
//         {
//             elements.push_back(ASTRenameQuery::Element());
//             elements.back().from.database = mapped_to_database;
//             elements.back().from.table = rename_element.from.table;
//             elements.back().to.database = mapped_to_database;
//             elements.back().to.table = rename_element.to.table;
//         }
//     }

//     if (elements.empty())
//         return ASTs{};

//     auto rewritten_query = std::make_shared<ASTRenameQuery>();
//     rewritten_query->elements = elements;
//     return ASTs{rewritten_query};
// }

// void InterpreterAlterImpl::validate(const InterpreterAlterImpl::TQuery & /*query*/, ContextPtr /*context*/)
// {
// }

// ASTs InterpreterAlterImpl::getRewrittenQueries(
//     const InterpreterAlterImpl::TQuery & alter_query, ContextPtr context, const String & mapped_to_database, const String & mysql_database)
// {
//     if (resolveDatabase(alter_query.database, mysql_database, mapped_to_database, context) != mapped_to_database)
//         return {};

//     auto rewritten_alter_query = std::make_shared<ASTAlterQuery>();
//     auto rewritten_rename_query = std::make_shared<ASTRenameQuery>();
//     rewritten_alter_query->database = mapped_to_database;
//     rewritten_alter_query->table = alter_query.table;
//     rewritten_alter_query->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
//     rewritten_alter_query->set(rewritten_alter_query->command_list, std::make_shared<ASTExpressionList>());

//     auto dialect_type = ParserSettings::valueOf(context->getSettingsRef().dialect_type);

//     String default_after_column;
//     for (const auto & command_query : alter_query.command_list->children)
//     {
//         const auto & alter_command = command_query->as<MySQLParser::ASTAlterCommand>();

//         if (alter_command->type == MySQLParser::ASTAlterCommand::ADD_COLUMN)
//         {
//             const auto & additional_columns_name_and_type = getColumnsList(alter_command->additional_columns);
//             const auto & additional_columns_description = createColumnsDescription(additional_columns_name_and_type, alter_command->additional_columns);
//             const auto & additional_columns = InterpreterCreateQuery::formatColumns(additional_columns_description, dialect_type);

//             for (size_t index = 0; index < additional_columns_name_and_type.size(); ++index)
//             {
//                 auto rewritten_command = std::make_shared<ASTAlterCommand>();
//                 rewritten_command->type = ASTAlterCommand::ADD_COLUMN;
//                 rewritten_command->if_not_exists = true;
//                 rewritten_command->first = alter_command->first;
//                 rewritten_command->col_decl = additional_columns->children[index]->clone();

//                 const auto & column_declare = alter_command->additional_columns->children[index]->as<MySQLParser::ASTDeclareColumn>();
//                 if (column_declare && column_declare->column_options)
//                 {
//                     /// We need to add default expression for fill data
//                     const auto & column_options = column_declare->column_options->as<MySQLParser::ASTDeclareOptions>();

//                     const auto & default_expression_it = column_options->changes.find("default");
//                     if (default_expression_it != column_options->changes.end())
//                     {
//                         ASTColumnDeclaration * col_decl = rewritten_command->col_decl->as<ASTColumnDeclaration>();
//                         col_decl->default_specifier = "DEFAULT";
//                         col_decl->default_expression = default_expression_it->second->clone();
//                         col_decl->children.emplace_back(col_decl->default_expression);
//                     }
//                 }

//                 if (default_after_column.empty())
//                 {
//                     StoragePtr storage = DatabaseCatalog::instance().getTable({mapped_to_database, alter_query.table}, context);
//                     Block storage_header = storage->getInMemoryMetadataPtr()->getSampleBlock();

//                     /// Put the sign and version columns last
//                     default_after_column = storage_header.getByPosition(storage_header.columns() - 1).name;
//                 }

//                 if (!alter_command->column_name.empty())
//                 {
//                     rewritten_command->column = std::make_shared<ASTIdentifier>(alter_command->column_name);
//                     rewritten_command->children.push_back(rewritten_command->column);

//                     /// For example(when add_column_1 is last column):
//                     /// ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_3 INT AFTER add_column_1, ADD COLUMN add_column_4 INT
//                     /// In this case, we still need to change the default after column

//                     if (alter_command->column_name == default_after_column)
//                         default_after_column = rewritten_command->col_decl->as<ASTColumnDeclaration>()->name;
//                 }
//                 else
//                 {
//                     rewritten_command->column = std::make_shared<ASTIdentifier>(default_after_column);
//                     rewritten_command->children.push_back(rewritten_command->column);
//                     default_after_column = rewritten_command->col_decl->as<ASTColumnDeclaration>()->name;
//                 }

//                 rewritten_command->children.push_back(rewritten_command->col_decl);
//                 rewritten_alter_query->command_list->children.push_back(rewritten_command);
//             }
//         }
//         else if (alter_command->type == MySQLParser::ASTAlterCommand::DROP_COLUMN)
//         {
//             auto rewritten_command = std::make_shared<ASTAlterCommand>();
//             rewritten_command->type = ASTAlterCommand::DROP_COLUMN;
//             rewritten_command->if_exists = true;
//             rewritten_command->column = std::make_shared<ASTIdentifier>(alter_command->column_name);
//             rewritten_alter_query->command_list->children.push_back(rewritten_command);
//         }
//         else if (alter_command->type == MySQLParser::ASTAlterCommand::RENAME_COLUMN)
//         {
//             if (alter_command->old_name != alter_command->column_name)
//             {
//                 /// 'RENAME column_name TO column_name' is not allowed in Clickhouse
//                 auto rewritten_command = std::make_shared<ASTAlterCommand>();
//                 rewritten_command->type = ASTAlterCommand::RENAME_COLUMN;
//                 rewritten_command->column = std::make_shared<ASTIdentifier>(alter_command->old_name);
//                 rewritten_command->rename_to = std::make_shared<ASTIdentifier>(alter_command->column_name);
//                 rewritten_alter_query->command_list->children.push_back(rewritten_command);
//             }
//         }
//         else if (alter_command->type == MySQLParser::ASTAlterCommand::MODIFY_COLUMN)
//         {
//             String new_column_name;

//             {
//                 auto rewritten_command = std::make_shared<ASTAlterCommand>();
//                 rewritten_command->type = ASTAlterCommand::MODIFY_COLUMN;
//                 rewritten_command->first = alter_command->first;
//                 auto modify_columns = getColumnsList(alter_command->additional_columns);

//                 if (modify_columns.size() != 1)
//                     throw Exception("It is a bug", ErrorCodes::LOGICAL_ERROR);

//                 new_column_name = modify_columns.front().name;

//                 if (!alter_command->old_name.empty())
//                     modify_columns.front().name = alter_command->old_name;

//                 const auto & modify_columns_description = createColumnsDescription(modify_columns, alter_command->additional_columns);
//                 rewritten_command->col_decl = InterpreterCreateQuery::formatColumns(modify_columns_description, dialect_type)->children[0];

//                 if (!alter_command->column_name.empty())
//                 {
//                     rewritten_command->column = std::make_shared<ASTIdentifier>(alter_command->column_name);
//                     rewritten_command->children.push_back(rewritten_command->column);
//                 }

//                 rewritten_alter_query->command_list->children.push_back(rewritten_command);
//             }

//             if (!alter_command->old_name.empty() && alter_command->old_name != new_column_name)
//             {
//                 auto rewritten_command = std::make_shared<ASTAlterCommand>();
//                 rewritten_command->type = ASTAlterCommand::RENAME_COLUMN;
//                 rewritten_command->column = std::make_shared<ASTIdentifier>(alter_command->old_name);
//                 rewritten_command->rename_to = std::make_shared<ASTIdentifier>(new_column_name);
//                 rewritten_alter_query->command_list->children.push_back(rewritten_command);
//             }
//         }
//         else if (alter_command->type == MySQLParser::ASTAlterCommand::RENAME_TABLE)
//         {
//             const auto & to_database = resolveDatabase(alter_command->new_database_name, mysql_database, mapped_to_database, context);

//             if (to_database != mapped_to_database)
//                 throw Exception("Cannot rename with other database for external ddl query.", ErrorCodes::NOT_IMPLEMENTED);

//             /// For ALTER TABLE table_name RENAME TO new_table_name_1, RENAME TO new_table_name_2;
//             /// We just need to generate RENAME TABLE table_name TO new_table_name_2;
//             if (rewritten_rename_query->elements.empty())
//                 rewritten_rename_query->elements.push_back(ASTRenameQuery::Element());

//             rewritten_rename_query->elements.back().from.database = mapped_to_database;
//             rewritten_rename_query->elements.back().from.table = alter_query.table;
//             rewritten_rename_query->elements.back().to.database = mapped_to_database;
//             rewritten_rename_query->elements.back().to.table = alter_command->new_table_name;
//         }
//     }

//     ASTs rewritten_queries;

//     /// Order is very important. We always execute alter first and then execute rename
//     if (!rewritten_alter_query->command_list->children.empty())
//         rewritten_queries.push_back(rewritten_alter_query);

//     if (!rewritten_rename_query->elements.empty())
//         rewritten_queries.push_back(rewritten_rename_query);

//     return rewritten_queries;
// }

}

}
