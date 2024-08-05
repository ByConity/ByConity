/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <memory>

#include <filesystem>

#include "Common/Exception.h"
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include <Common/randomSeed.h>
#include <Common/atomicRename.h>

#include <Core/Defines.h>
#include <Core/Settings.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/formatTenantDatabaseName.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMaterializedView.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/join_common.h>

#include <Access/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeBitMap64.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseCnch.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <common/logger_useful.h>
#include <DataTypes/ObjectUtils.h>
#include <Parsers/queryToString.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Storages/StorageCnchMergeTree.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>

#include <Catalog/Catalog.h>
#include <ExternalCatalog/IExternalCatalogMgr.h>
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>

#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Optimizer/QueryUseOptimizerChecker.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int CATALOG_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_DATABASE_FOR_TEMPORARY_TABLE;
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int PATH_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace fs = std::filesystem;

InterpreterCreateQuery::InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
    /// if the settting is enabled and query are from user
    if (context_->getSettingsRef().enable_cnch_engine_conversion && context_->getInitialQueryId().size())
        query_ptr = convertMergeTreeToCnchEngine(std::move(query_ptr));
}

static void createPlainConfigFromSettings(const ASTSetQuery & settings, ExternalCatalog::PlainConfigs & conf)
{
    for (const auto & change : settings.changes)
    {
        conf.setString(change.name, toString(change.value));
    }
}


BlockIO InterpreterCreateQuery::createExternalCatalog(ASTCreateQuery & create)
{
    String catalog_name = create.catalog;
    if (catalog_name.empty())
        throw Exception("Database name is empty while creating database", ErrorCodes::BAD_ARGUMENTS);
    //TODO(ExterncalCatalog):: add intent lock.
    if (ExternalCatalog::Mgr::instance().isCatalogExist(catalog_name))
    {
        if (create.if_not_exists)
            return {};
        else
            throw Exception("Catalog " + catalog_name + " already exists.", ErrorCodes::CATALOG_ALREADY_EXISTS);
    }

    ExternalCatalog::PlainConfigsPtr conf_ptr(new ExternalCatalog::PlainConfigs());
    createPlainConfigFromSettings(*create.catalog_properties, *conf_ptr);

    bool created = ExternalCatalog::Mgr::instance().createCatalog(catalog_name, conf_ptr.get(), TxnTimestamp(getContext()->getTimestamp()));
    if (created)
        return {};
    else
        throw Exception("Catalog " + catalog_name + " could not be created", ErrorCodes::LOGICAL_ERROR);
}


BlockIO InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
    String database_name = create.database;
    if (database_name.empty())
        throw Exception("Database name is empty while creating database", ErrorCodes::BAD_ARGUMENTS);

    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    /// Database can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard
    if (DatabaseCatalog::instance().isDatabaseExist(database_name, getContext()))
    {
        if (create.if_not_exists)
            return {};
        else
            throw Exception("Database " + database_name + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
    }

    /// Will write file with database metadata, if needed.
    String database_name_escaped = escapeForFileName(database_name);
    fs::path metadata_path = fs::canonical(getContext()->getPath());
    fs::path metadata_file_tmp_path = metadata_path / "metadata" / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = metadata_path / "metadata" / (database_name_escaped + ".sql");

    if (!create.storage && create.attach)
    {
        if (!fs::exists(metadata_file_path))
            throw Exception("Database engine must be specified for ATTACH DATABASE query", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        /// Short syntax: try read database definition from file
        auto ast = DatabaseOnDisk::parseQueryFromMetadata(nullptr, getContext(), metadata_file_path);
        create = ast->as<ASTCreateQuery &>();
        if (!create.table.empty() || !create.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Metadata file {} contains incorrect CREATE DATABASE query", metadata_file_path.string());
        create.attach = true;
        create.attach_short_syntax = true;
        create.database = database_name;
    }
    else if (!create.storage)
    {
        /// For new-style databases engine is explicitly specified in .sql
        /// When attaching old-style database during server startup, we must always use Ordinary engine
        if (create.attach)
            throw Exception("Database engine must be specified for ATTACH DATABASE query", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
        DefaultDatabaseEngine default_database_engine = (database_name == "default") ? DefaultDatabaseEngine::Atomic : getContext()->getSettingsRef().default_database_engine.value;
        auto engine = std::make_shared<ASTFunction>();
        auto storage = std::make_shared<ASTStorage>();

        // TODO: check here
        if (default_database_engine == DefaultDatabaseEngine::Cnch)
            engine->name = "Cnch";
        else if (default_database_engine == DefaultDatabaseEngine::Ordinary)
            engine->name = "Ordinary";
        else if (default_database_engine == DefaultDatabaseEngine::Atomic)
            engine->name = "Atomic";
        else
            engine->name = "Memory";

        engine->no_empty_args = true;
        storage->set(storage->engine, engine);
        create.set(create.storage, storage);
    }
    else if ((create.columns_list
              && ((create.columns_list->indices && !create.columns_list->indices->children.empty())
                  || (create.columns_list->projections && !create.columns_list->projections->children.empty()))))
    {
        /// Currently, there are no database engines, that support any arguments.
        throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE, "Unknown database engine: {}", serializeAST(*create.storage));
    }

    if (create.storage->engine->name == "Atomic" || create.storage->engine->name == "Replicated" || create.storage->engine->name == "MaterializedPostgreSQL" || create.storage->engine->name == "Cnch")
    {
        if (create.storage->engine->name == "Cnch" && database_name == "default")
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Create default database with Cnch Engine is not allowed.");
        if (create.attach && create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "UUID must be specified for ATTACH. "
                            "If you want to attach existing database, use just ATTACH DATABASE {};", create.database);
        else if (create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();

        metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);

        if (!create.attach && fs::exists(metadata_path))
            throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists", metadata_path.string());
    }
    else if (create.storage->engine->name == "CnchMaterializedMySQL" || create.storage->engine->name == "CloudMaterializedMySQL")
    {
        /// It creates nested database with Ordinary or Atomic engine depending on UUID in query and default engine setting.
        /// Do nothing if it's an internal ATTACH on server startup or short-syntax ATTACH query from user,
        /// because we got correct query from the metadata file in this case.
        /// If we got query from user, then normalize it first.
        bool attach_from_user = create.attach && !internal && !create.attach_short_syntax;
        bool create_from_user = !create.attach;

        if (create_from_user && create.uuid == UUIDHelpers::Nil)
            create.uuid = UUIDHelpers::generateV4();
        else if (attach_from_user && create.uuid == UUIDHelpers::Nil)
        {
            /// Ambiguity is possible: should we attach nested database as Ordinary
            /// or throw "UUID must be specified" for Atomic? So we suggest short syntax for Ordinary.
            throw Exception("Use short attach syntax ('ATTACH DATABASE name;' without engine) to attach existing database "
                            "or specify UUID to attach new database with Atomic engine", ErrorCodes::INCORRECT_QUERY);
        }

        /// Set metadata path according to nested engine
        metadata_path = metadata_path / "store" / DatabaseCatalog::getPathForUUID(create.uuid);
    }
    else
    {
        bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        if (create.uuid != UUIDHelpers::Nil && !is_on_cluster)
            throw Exception("Ordinary database engine does not support UUID", ErrorCodes::INCORRECT_QUERY);

        /// Ignore UUID if it's ON CLUSTER query
        create.uuid = UUIDHelpers::Nil;
        metadata_path = metadata_path / "metadata" / database_name_escaped;
    }

    if (create.storage->engine->name == "CnchMaterializedMySQL" && !getContext()->getSettingsRef().allow_experimental_database_materialize_mysql
        && !internal)
    {
        throw Exception("CnchMaterializedMySQL is an experimental database engine. "
                        "Enable allow_experimental_database_materialize_mysql to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    if (create.storage->engine->name == "Replicated" && !getContext()->getSettingsRef().allow_experimental_database_replicated && !internal)
    {
        throw Exception("Replicated is an experimental database engine. "
                        "Enable allow_experimental_database_replicated to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    if (create.storage->engine->name == "MaterializedPostgreSQL" && !getContext()->getSettingsRef().allow_experimental_database_materialized_postgresql && !internal)
    {
        throw Exception("MaterializedPostgreSQL is an experimental database engine. "
                        "Enable allow_experimental_database_postgresql_replica to use it.", ErrorCodes::UNKNOWN_DATABASE_ENGINE);
    }

    DatabasePtr database = DatabaseFactory::get(create, metadata_path / "", getContext());
    if (const auto * database_cnch = dynamic_cast<const DatabaseCnch *>(database.get()))
    {
        if (create.attach || create.attach_short_syntax || create.attach_from_path)
            throw Exception("Cnch database doesn't support ATTACH", ErrorCodes::SUPPORT_IS_DISABLED);
        auto txn = getContext()->getCurrentTransaction();
        if (!txn)
            throw Exception("Cnch transaction not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);
        /// Need to acquire kv lock before creating entry
        auto db_lock = txn->createIntentLock(IntentLock::DB_LOCK_PREFIX, database_name);
        db_lock->lock();
        if (DatabaseCatalog::instance().isDatabaseExist(database_name, getContext()))
        {
            if (create.if_not_exists)
                return {};
            else
                throw Exception("Database " + database_name + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
        }
        if (const auto * database_cnch_mmsql = dynamic_cast<const DatabaseCnchMaterializedMySQL *>(database.get()))
            database_cnch_mmsql->createEntryInCnchCatalog(getContext(), queryToString(create));
        else
            database_cnch->createEntryInCnchCatalog(getContext());
    }

    if (create.uuid != UUIDHelpers::Nil)
        create.database = TABLE_WITH_UUID_NAME_PLACEHOLDER;

    bool need_write_metadata_on_disk = (database->getEngineName() != "Cnch") && (database->getEngineName() != "CnchMaterializedMySQL")
                                        && (database->getEngineName() != "CloudMaterializedMySQL") && (!create.attach || !fs::exists(metadata_file_path));

    if (need_write_metadata_on_disk)
    {
        create.attach = true;
        create.if_not_exists = false;

        WriteBufferFromOwnString statement_buf;
        formatAST(create, statement_buf, false);
        writeChar('\n', statement_buf);
        String statement = statement_buf.str();

        /// Exclusive flag guarantees, that database is not created right now in another thread.
        WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (getContext()->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    /// We attach database before loading it's tables, so do not allow concurrent DDL queries
    auto db_guard = DatabaseCatalog::instance().getExclusiveDDLGuardForDatabase(database_name);

    bool added = false;
    bool renamed = false;
    try
    {
        /// TODO Attach db only after it was loaded. Now it's not possible because of view dependencies
        DatabaseCatalog::instance().attachDatabase(database_name, database);
        added = true;

        if (need_write_metadata_on_disk)
        {
            /// Prevents from overwriting metadata of detached database
            renameNoReplace(metadata_file_tmp_path, metadata_file_path);
            renamed = true;
        }

        /// We use global context here, because storages lifetime is bigger than query context lifetime
        database->loadStoredObjects(getContext()->getGlobalContext(), has_force_restore_data_flag, create.attach && force_attach); //-V560
    }
    catch (...)
    {
        if (renamed)
        {
            [[maybe_unused]] bool removed = fs::remove(metadata_file_path);
            assert(removed);
        }
        if (added)
            DatabaseCatalog::instance().detachDatabase(getContext(), database_name, false, false);

        throw;
    }

    return {};
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns, ParserSettingsImpl parser_settings)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column.name;

        ParserDataType type_parser(parser_settings);
        String type_name = column.type->getName();
        const char * pos = type_name.data();
        const char * end = pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, pos, end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        column_declaration->flags = column.type->getFlags();
        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns, ParserSettingsImpl parser_settings)
{
    std::shared_ptr<ASTExpressionList> columns_list =
            std::static_pointer_cast<ASTExpressionList>(formatColumns(columns, parser_settings));

    for (const auto & alias_column : alias_columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = alias_column.name;

        ParserDataType type_parser(parser_settings);
        String type_name = alias_column.type->getName();
        const char * type_pos = type_name.data();
        const char * type_end = type_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_pos, type_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        column_declaration->flags = alias_column.type->getFlags();

        column_declaration->default_specifier = "ALIAS";

        const auto & alias = alias_column.expression;
        const char * alias_pos = alias.data();
        const char * alias_end = alias_pos + alias.size();
        ParserExpression expression_parser(parser_settings);
        column_declaration->default_expression = parseQuery(expression_parser, alias_pos, alias_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(const ColumnsDescription & columns, ParserSettingsImpl parser_settings)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;

        ParserDataType type_parser(parser_settings);
        String type_name = column.type->getName();
        const char * type_name_pos = type_name.data();
        const char * type_name_end = type_name_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_name_pos, type_name_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        column_declaration->flags = column.type->getFlags();

        if (column.default_desc.expression)
        {
            column_declaration->default_specifier = toString(column.default_desc.kind);
            column_declaration->default_expression = column.default_desc.expression->clone();
        }

        if (!column.comment.empty())
        {
            column_declaration->comment = std::make_shared<ASTLiteral>(Field(column.comment));
        }

        if (column.codec)
            column_declaration->codec = column.codec;

        if (column.ttl)
            column_declaration->ttl = column.ttl;

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}

ASTPtr InterpreterCreateQuery::formatIndices(const IndicesDescription & indices)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices)
        res->children.push_back(index.definition_ast->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatConstraints(const ConstraintsDescription & constraints)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & constraint : constraints.constraints)
        res->children.push_back(constraint->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatForeignKeys(const ForeignKeysDescription & foreign_keys)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & foreign_key : foreign_keys.foreign_keys)
        res->children.push_back(foreign_key->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatUnique(const UniqueNotEnforcedDescription & unique)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & unique_key : unique.unique)
        res->children.push_back(unique_key->clone());

    return res;
}

ASTPtr InterpreterCreateQuery::formatProjections(const ProjectionsDescription & projections)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & projection : projections)
        res->children.push_back(projection.definition_ast->clone());

    return res;
}

ColumnsDescription InterpreterCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns_ast, ContextPtr context_, bool attach, bool system)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;
    /// Only applies default nullable transform if:
    /// 1. The query is not a system query, such as: system.query_log, system.query_thread_log
    /// 2. The query is to create a new table rather than attaching an existed table.
    bool make_columns_nullable = !system && !attach && context_->getSettingsRef().data_type_default_nullable;

    for (const auto & ast : columns_ast.children)
    {
        const auto & col_decl = ast->as<ASTColumnDeclaration &>();

        DataTypePtr column_type = nullptr;

        if (col_decl.type)
        {
            column_type = DataTypeFactory::instance().get(col_decl.type);

            if (col_decl.unsigned_modifier)
            {
                if (!isInteger(column_type))
                    throw Exception("Can only use SIGNED/UNSIGNED modifier with integer type", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
                if (isSignedInteger(column_type) && *col_decl.unsigned_modifier)
                    column_type = makeUnsigned(column_type);
                if (isUnsignedInteger(column_type) && !(*col_decl.unsigned_modifier))
                    column_type = makeSigned(column_type);
            }

            if (col_decl.null_modifier)
            {
                if (column_type->isNullable())
                    throw Exception("Can't use [NOT] NULL modifier with Nullable type", ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
                if (*col_decl.null_modifier)
                    column_type = JoinCommon::convertTypeToNullable(column_type);
            }
            else if (make_columns_nullable)
            {
                column_type = JoinCommon::convertTypeToNullable(column_type);
            }

            column_names_and_types.emplace_back(col_decl.name, column_type);
        }
        else
        {
            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            column_names_and_types.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());
        }

        /// add column to postprocessing if there is a default_expression specified
        if (col_decl.default_expression)
        {
            /** For columns with explicitly-specified type create two expressions:
              * 1. default_expression aliased as column name with _tmp suffix
              * 2. conversion of expression (1) to explicitly-specified type alias as column name
              */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
                const auto * data_type_ptr = column_names_and_types.back().type.get();

                default_expr_list->children.emplace_back(
                    setAlias(addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                        final_column_name));

                default_expr_list->children.emplace_back(
                    setAlias(
                        col_decl.default_expression->clone(),
                        tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }

        // Set type flags based on information in AST
        column_names_and_types.back().type->setFlags(col_decl.flags);
    }

    Block defaults_sample_block;
    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!default_expr_list->children.empty())
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_list, column_names_and_types, context_);

    bool sanity_check_compression_codecs = !attach && !context_->getSettingsRef().allow_suspicious_codecs;
    bool allow_experimental_codecs = attach || context_->getSettingsRef().allow_experimental_codecs;

    ColumnsDescription res;
    auto name_type_it = column_names_and_types.begin();
    for (auto ast_it = columns_ast.children.begin(); ast_it != columns_ast.children.end(); ++ast_it, ++name_type_it)
    {
        ColumnDescription column;

        auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();

        column.name = col_decl.name;

        if (col_decl.default_expression)
        {
            ASTPtr default_expr = col_decl.default_expression->clone();
            if (col_decl.type)
                column.type = name_type_it->type;
            else
            {
                column.type = defaults_sample_block.getByName(column.name).type;
                /// set nullability for case of column declaration w/o type but with default expression
                if ((col_decl.null_modifier && *col_decl.null_modifier) || make_columns_nullable)
                    column.type = makeNullable(column.type);
            }
            column.default_desc.kind = columnDefaultKindFromString(col_decl.default_specifier);
            column.default_desc.expression = default_expr;
        }
        else if (col_decl.type)
            column.type = name_type_it->type;
        else
            throw Exception();

        if (col_decl.comment)
            column.comment = col_decl.comment->as<ASTLiteral &>().value.get<String>();

        if (col_decl.codec)
        {
            if (col_decl.default_specifier == "ALIAS")
                throw Exception{"Cannot specify codec for column type ALIAS", ErrorCodes::BAD_ARGUMENTS};
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                col_decl.codec, column.type, sanity_check_compression_codecs, allow_experimental_codecs);
        }

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        res.add(std::move(column));
    }

    if (!attach && context_->getSettingsRef().flatten_nested)
        res.flattenNested();

    if (res.getAllPhysical().empty())
        throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

    return res;
}


ConstraintsDescription InterpreterCreateQuery::getConstraintsDescription(const ASTExpressionList * constraints)
{
    ConstraintsDescription res;
    if (constraints)
        for (const auto & constraint : constraints->children)
            res.constraints.push_back(std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint->clone()));
    return res;
}

ForeignKeysDescription InterpreterCreateQuery::getForeignKeysDescription(const ASTExpressionList * foreign_keys)
{
    ForeignKeysDescription res;
    if (foreign_keys)
        for (const auto & foreign_key : foreign_keys->children)
            res.foreign_keys.push_back(std::dynamic_pointer_cast<ASTForeignKeyDeclaration>(foreign_key->clone()));
    return res;
}

UniqueNotEnforcedDescription InterpreterCreateQuery::getUniqueNotEnforcedDescription(const ASTExpressionList * unique)
{
    UniqueNotEnforcedDescription res;
    if (unique)
        for (const auto & unique_key : unique->children)
            res.unique.push_back(std::dynamic_pointer_cast<ASTUniqueNotEnforcedDeclaration>(unique_key->clone()));
    return res;
}

InterpreterCreateQuery::TableProperties InterpreterCreateQuery::setProperties(ASTCreateQuery & create) const
{
    TableProperties properties;
    TableLockHolder as_storage_lock;

    auto check_view_columns_same_with_select = [&](const ColumnsDescription & columns) {
        if (!create.attach && create.is_ordinary_view && create.select && getContext()->getSettingsRef().create_view_check_column_names)
        {
            Block select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(), getContext());
            Names select_names = select_sample.getNames();
            Names column_names = columns.getNamesOfOrdinary();

            if (select_names != column_names)
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Columns of select query are not same with the column list, select query: {}, columns list: {}",
                    fmt::join(select_names, ","),
                    fmt::join(column_names, ","));
        }
    };

    if (create.columns_list)
    {
        if (create.as_table_function
            && (create.columns_list->indices || create.columns_list->constraints || create.columns_list->foreign_keys
                || create.columns_list->unique))
            throw Exception(
                "Indexes, constraints and foreign keys and unique(not enforced) are not supported for table functions",
                ErrorCodes::INCORRECT_QUERY);

        if (create.columns_list->columns)
        {
            properties.columns = getColumnsDescription(*create.columns_list->columns, getContext(), create.attach, internal);
            check_view_columns_same_with_select(properties.columns);
        }

        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
                properties.indices.push_back(
                    IndexDescription::getIndexFromAST(index->clone(), properties.columns, getContext()));

        if (create.columns_list->projections)
            for (const auto & projection_ast : create.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, properties.columns, getContext());
                properties.projections.add(std::move(projection));
            }

        properties.constraints = getConstraintsDescription(create.columns_list->constraints);
        properties.foreign_keys = getForeignKeysDescription(create.columns_list->foreign_keys);
        properties.unique = getUniqueNotEnforcedDescription(create.columns_list->unique);
    }
    else if (!create.as_table.empty())
    {
        String as_database_name = getContext()->resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, getContext());

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr();
        properties.columns = as_storage_metadata->getColumns();
        check_view_columns_same_with_select(properties.columns);

        /// Secondary indices make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
            properties.indices = as_storage_metadata->getSecondaryIndices();

        /// Create table as should set projections
        if (as_storage_metadata->hasProjections())
            properties.projections = as_storage_metadata->getProjections().clone();

        properties.constraints = as_storage_metadata->getConstraints();
        properties.foreign_keys = as_storage_metadata->getForeignKeys();
        properties.unique = as_storage_metadata->getUniqueNotEnforced();
    }
    else if (create.select)
    {
        Block as_select_sample;
        if (create.is_ordinary_view)
        {
            auto cloned_query = create.select->clone();
            if (QueryUseOptimizerChecker::check(cloned_query, getContext()))
                as_select_sample = InterpreterSelectQueryUseOptimizer(
                                       cloned_query, getContext(), SelectQueryOptions().analyze().setWithoutExtendedObject())
                                       .getSampleBlock();
            else
                as_select_sample
                    = InterpreterSelectWithUnionQuery(cloned_query, getContext(), SelectQueryOptions().analyze().setWithoutExtendedObject())
                          .getSampleBlock();
        }
        else
            as_select_sample = InterpreterSelectWithUnionQuery(
                                   create.select->clone(), getContext(), SelectQueryOptions().analyze().setWithoutExtendedObject())
                                   .getSampleBlock();
        properties.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function = TableFunctionFactory::instance().get(create.as_table_function, getContext());
        properties.columns = table_function->getActualTableStructure(getContext());
        check_view_columns_same_with_select(properties.columns);
        assert(!properties.columns.empty());
    }
    else if (create.is_dictionary)
    {
        return {};
    }
    else if (!create.storage || !create.storage->engine)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected application state. CREATE query is missing either its storage or engine.");
    /// We can have queries like "CREATE TABLE <table> ENGINE=<engine>" if <engine>
    /// supports schema inference (will determine table structure in it's constructor).
    else if (!StorageFactory::instance().checkIfStorageSupportsSchemaInference(create.storage->engine->name))
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

    if (create.ignore_bitengine_encode)
    {
        /// there's no bitengine columns in table, just reset the flag
        if (0 == processIgnoreBitEngineEncode(properties.columns))
            create.ignore_bitengine_encode = false;
    }

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    if (!create.columns_list)
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    ParserSettingsImpl dialect_type = ParserSettings::valueOf(getContext()->getSettingsRef().dialect_type);
    ASTPtr new_columns = formatColumns(properties.columns, dialect_type);
    ASTPtr new_indices = formatIndices(properties.indices);
    ASTPtr new_constraints = formatConstraints(properties.constraints);
    ASTPtr new_foreign_keys = formatForeignKeys(properties.foreign_keys);
    ASTPtr new_unique = formatUnique(properties.unique);
    ASTPtr new_projections = formatProjections(properties.projections);

    create.columns_list->setOrReplace(create.columns_list->columns, new_columns);
    create.columns_list->setOrReplace(create.columns_list->indices, new_indices);
    create.columns_list->setOrReplace(create.columns_list->constraints, new_constraints);
    create.columns_list->setOrReplace(create.columns_list->foreign_keys, new_foreign_keys);
    create.columns_list->setOrReplace(create.columns_list->unique, new_unique);
    create.columns_list->setOrReplace(create.columns_list->projections, new_projections);

    validateTableStructure(create, properties);
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    assert(as_database_saved.empty() && as_table_saved.empty());
    std::swap(create.as_database, as_database_saved);
    std::swap(create.as_table, as_table_saved);

    return properties;
}

void InterpreterCreateQuery::validateTableStructure(const ASTCreateQuery & create,
                                                    const InterpreterCreateQuery::TableProperties & properties) const
{
    /// Check for duplicates
    std::set<String> all_columns;

    const auto & settings = getContext()->getSettingsRef();

    /// Check low cardinality types in creating table if it was not allowed in setting
    if (!create.attach && !settings.allow_suspicious_low_cardinality_types && !create.is_materialized_view)
    {
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
        {
            if (const auto * current_type_ptr = typeid_cast<const DataTypeLowCardinality *>(name_and_type_pair.type.get()))
            {
                if (!isStringOrFixedString(*removeNullable(current_type_ptr->getDictionaryType())))
                    throw Exception("Creating columns of type " + current_type_ptr->getName() + " is prohibited by default "
                                    "due to expected negative impact on performance. "
                                    "It can be enabled with the \"allow_suspicious_low_cardinality_types\" setting.",
                                    ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY);
            }
        }
    }

    if (!create.attach && !settings.allow_experimental_geo_types)
    {
        for (const auto & name_and_type_pair : properties.columns.getAllPhysical())
        {
            const auto & type = name_and_type_pair.type->getName();
            if (type == "MultiPolygon" || type == "Polygon" || type == "Ring" || type == "Point")
            {
                String message = "Cannot create table with column '" + name_and_type_pair.name + "' which type is '"
                                 + type + "' because experimental geo types are not allowed. "
                                 + "Set setting allow_experimental_geo_types = 1 in order to allow it.";
                throw Exception(message, ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }

    if (!create.attach && !settings.allow_experimental_object_type)
    {
        for (const auto & [name, type] : properties.columns.getAllPhysical())
        {
            if (isObject(type))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create table with column '{}' which type is '{}' "
                    "because experimental Object type is not allowed. "
                    "Set setting allow_experimental_object_type = 1 in order to allow it",
                    name, type->getName());
            }
        }
    }
}

void InterpreterCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.as_table_function)
        return;

    if (create.storage || create.is_dictionary || create.isView())
    {
        if (create.temporary && create.storage && create.storage->engine && create.storage->engine->name != "Memory")
            throw Exception(
                "Temporary tables can only be created with ENGINE = Memory, not " + create.storage->engine->name,
                ErrorCodes::INCORRECT_QUERY);

        /// CnchHive only when create table need to check hive schema.
        if(create.storage && create.storage->engine && create.storage->engine->name == "CnchHive")
            create.create = true;

        return;
    }

    if (create.temporary)
    {
        auto engine_ast = std::make_shared<ASTFunction>();
        engine_ast->name = "Memory";
        engine_ast->no_empty_args = true;
        auto storage_ast = std::make_shared<ASTStorage>();
        storage_ast->set(storage_ast->engine, engine_ast);
        create.set(create.storage, storage_ast);
    }
    else if (!create.as_table.empty())
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = getContext()->resolveDatabase(create.as_database);
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name, getContext())->getCreateTableQuery(as_table_name, getContext());
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        const String qualified_name = backQuoteIfNeed(as_database_name) + "." + backQuoteIfNeed(as_table_name);

        if (as_create.is_ordinary_view)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a View",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.is_live_view)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a Live View",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.is_dictionary)
            throw Exception(
                "Cannot CREATE a table AS " + qualified_name + ", it is a Dictionary",
                ErrorCodes::INCORRECT_QUERY);

        if (as_create.storage)
        {
            if (create.ignore_replicated)
            {
                bool is_ha = startsWith(as_create.storage->engine->name, "Ha");
                bool is_replicated = startsWith(as_create.storage->engine->name, "Replicated");

                if (is_ha || is_replicated)
                {
                    as_create.storage->engine->name = as_create.storage->engine->name.substr(is_replicated ? 10 : 2);
                    as_create.storage->engine->arguments = nullptr;
                    as_create.storage->engine->parameters = nullptr;
                }
            }

            if (create.ignore_async && as_create.storage->settings)
            {
                auto & setting_changes = as_create.storage->settings->changes;
                for (auto itr = setting_changes.begin(); itr != setting_changes.end();  )
                {
                    if (itr->name == "enable_async_init_metasotre")
                        itr = setting_changes.erase(itr);
                    else
                        ++itr;
                }
            }

            if (create.ignore_ttl && as_create.storage->ttl_table)
                as_create.storage->ttl_table = nullptr;

            create.set(create.storage, as_create.storage->ptr());
            if (as_create.comment)
                create.set(create.comment, as_create.comment->ptr());
        }
        else if (as_create.as_table_function)
            create.as_table_function = as_create.as_table_function->clone();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set engine, it's a bug.");
    }
}

static void generateUUIDForTable(ASTCreateQuery & create)
{
    if (create.uuid == UUIDHelpers::Nil)
        create.uuid = UUIDHelpers::generateV4();

    /// If destination table (to_table_id) is not specified for materialized view,
    /// then MV will create inner table. We should generate UUID of inner table here,
    /// so it will be the same on all hosts if query in ON CLUSTER or database engine is Replicated.
    bool need_uuid_for_inner_table = !create.attach && create.is_materialized_view && !create.to_table_id;
    if (need_uuid_for_inner_table && create.to_inner_uuid == UUIDHelpers::Nil)
        create.to_inner_uuid = UUIDHelpers::generateV4();
}

void InterpreterCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    ///const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";

    if (database->getEngineName() == "Replicated" && getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
        && !internal)
    {
        if (create.uuid == UUIDHelpers::Nil)
            throw Exception("Table UUID is not specified in DDL log", ErrorCodes::LOGICAL_ERROR);
    }

    bool from_path = create.attach_from_path.has_value();

    if (database->getUUID() != UUIDHelpers::Nil)
    {
        if (create.attach && !from_path && create.uuid == UUIDHelpers::Nil)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Incorrect ATTACH {} query for Atomic database engine. "
                            "Use one of the following queries instead:\n"
                            "1. ATTACH {} {};\n"
                            "2. CREATE {} {} <table definition>;\n"
                            "3. ATTACH {} {} FROM '/path/to/data/' <table definition>;\n"
                            "4. ATTACH {} {} UUID '<uuid>' <table definition>;",
                            kind_upper,
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table);
        }

        generateUUIDForTable(create);
    }
    /// CnchMergeTree shold always have the UUID.
    else if (database->getEngineName() == "Cnch")
    {
        generateUUIDForTable(create);
    }
    else
    {
        /// As table name is always not the same with it on server side,
        /// we may need UUID of table on worker side in Memory database engine to find CnchMergeTree when needed
        /// Anyway, here we may need some more graceful code

        //bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        //bool has_uuid = create.uuid != UUIDHelpers::Nil || create.to_inner_uuid != UUIDHelpers::Nil;
        //if (has_uuid && !is_on_cluster)
         //   throw Exception(ErrorCodes::INCORRECT_QUERY,
         //                   "{} UUID specified, but engine of database {} is not Atomic", kind, create.database);

        /// Ignore UUID if it's ON CLUSTER query
        //create.uuid = UUIDHelpers::Nil;
        //create.to_inner_uuid = UUIDHelpers::Nil;
    }

    if (create.replace_table)
    {
        if (database->getUUID() == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} query is supported only for Atomic databases",
                            create.create_or_replace ? "CREATE OR REPLACE TABLE" : "REPLACE TABLE");

        UUID uuid_of_table_to_replace;
        if (create.create_or_replace)
        {
            uuid_of_table_to_replace = getContext()->tryResolveStorageID(StorageID(create.database, create.table)).uuid;
            if (uuid_of_table_to_replace == UUIDHelpers::Nil)
            {
                /// Convert to usual CREATE
                create.replace_table = false;
                assert(!database->isTableExist(create.table, getContext()));
            }
            else
                create.table = "_tmp_replace_" + toString(uuid_of_table_to_replace);
        }
        else
        {
            uuid_of_table_to_replace = getContext()->resolveStorageID(StorageID(create.database, create.table)).uuid;
            if (uuid_of_table_to_replace == UUIDHelpers::Nil)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                                backQuoteIfNeed(create.database), backQuoteIfNeed(create.table));
            create.table = "_tmp_replace_" + toString(uuid_of_table_to_replace);
        }
    }
}


BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
    /// Temporary tables are created out of databases.
    if (create.temporary && !create.database.empty())
        throw Exception("Temporary tables cannot be inside a database. You should not specify a database for a temporary table.",
            ErrorCodes::BAD_DATABASE_FOR_TEMPORARY_TABLE);

    /// Need to judge create.columns_list to avoid possible core caused by:
    ///     CREATE TABLE u10104_t2 Engine=CnchMergeTree UNIQUE KEY(a) AS SELECT * FROM u10104_t1;
    if (create.storage && create.storage->unique_key && create.columns_list && create.columns_list->projections)
        throw Exception("`Projection` cannot be used together with `UNIQUE KEY`", ErrorCodes::BAD_ARGUMENTS);

    String current_database = getContext()->getCurrentDatabase();
    auto database_name = create.database.empty() ? current_database : create.database;

    // If this is a stub ATTACH query, read the query definition from the database
    if (create.attach && !create.storage && !create.columns_list)
    {
        auto database = DatabaseCatalog::instance().getDatabase(database_name, getContext());

        if (database->getEngineName() == "Replicated")
        {
            auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, create.table);

            if (auto* ptr = typeid_cast<DatabaseReplicated *>(database.get());
                ptr && !getContext()->getClientInfo().is_replicated_database_internal)
            {
                create.database = database_name;
                guard->releaseTableLock();
                return ptr->tryEnqueueReplicatedDDL(query_ptr, getContext());
            }
        }

        bool if_not_exists = create.if_not_exists;

        // Table SQL definition is available even if the table is detached (even permanently)
        auto query = database->getCreateTableQuery(create.table, getContext());
        auto create_query = query->as<ASTCreateQuery &>();

        if (!create.is_dictionary && create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH TABLE {}.{}, it is a Dictionary",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.table));

        if (create.is_dictionary && !create_query.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Cannot ATTACH DICTIONARY {}.{}, it is a Table",
                backQuoteIfNeed(database_name), backQuoteIfNeed(create.table));

        create = create_query; // Copy the saved create query, but use ATTACH instead of CREATE

        create.attach = true;
        create.attach_short_syntax = true;
        create.if_not_exists = if_not_exists;
    }

    /// TODO throw exception if !create.attach_short_syntax && !create.attach_from_path && !internal

    if (create.attach_from_path)
    {
        fs::path user_files = fs::path(getContext()->getUserFilesPath()).lexically_normal();
        fs::path root_path = fs::path(getContext()->getPath()).lexically_normal();

        if (getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            fs::path data_path = fs::path(*create.attach_from_path).lexically_normal();
            if (data_path.is_relative())
                data_path = (user_files / data_path).lexically_normal();
            if (!startsWith(data_path, user_files))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));

            /// Data path must be relative to root_path
            create.attach_from_path = fs::relative(data_path, root_path) / "";
        }
        else
        {
            fs::path data_path = (root_path / *create.attach_from_path).lexically_normal();
            if (!startsWith(data_path, user_files))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED,
                                "Data directory {} must be inside {} to attach it", String(data_path), String(user_files));
        }
    }
    else if (create.attach && !create.attach_short_syntax && getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        auto * log = &Poco::Logger::get("InterpreterCreateQuery");
        LOG_WARNING(log, "ATTACH TABLE query with full table definition is not recommended: "
                         "use either ATTACH TABLE {}; to attach existing table "
                         "or CREATE TABLE {} <table definition>; to create new table "
                         "or ATTACH TABLE {} FROM '/path/to/data/' <table definition>; to create new table and attach data.",
                         create.table, create.table, create.table);
    }

    if (!create.temporary && create.database.empty())
        create.database = current_database;
    if (create.to_table_id && create.to_table_id.database_name.empty())
        create.to_table_id.database_name = current_database;

    if (create.select && create.isView())
    {
        // Expand CTE before filling default database
        ApplyWithSubqueryVisitor().visit(*create.select);
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.select);
    }

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = setProperties(create);

    // Make sure names in foreign key exist.
    if (create.columns_list && create.columns_list->foreign_keys && !create.columns_list->foreign_keys->children.empty())
    {
        // When the reference column does not exist, foreign keys are still created
        bool force_create_foreign_key = getContext()->getSettingsRef().force_create_foreign_key;

        if (create.database.empty())
            create.database = getContext()->getCurrentDatabase();

        auto contains_columns = [](const ASTExpressionList & name_list, const Names & names) {
            NameSet name_set;
            for (const auto & name : names)
                name_set.insert(name);

            for (const auto & ptr : name_list.children)
                if (!name_set.contains(ptr->as<ASTIdentifier &>().name()))
                    return ptr->as<ASTIdentifier &>().name();
            return String();
        };

        Names columns;
        for (const auto & expr : create.columns_list->columns->as<ASTExpressionList &>().children)
            columns.push_back(expr->as<ASTColumnDeclaration &>().name);

        // FOREIGN KEY (foreign_key.column_names) REFERENCES(foreign_key.ref_column_names)
        if (!columns.empty())
        {
            const ASTExpressionList * foreign_keys = create.columns_list->foreign_keys;
            NameSet used_fk_names;

            for (const auto & foreign_key_child : foreign_keys->children)
            {
                auto & foreign_key = foreign_key_child->as<ASTForeignKeyDeclaration &>();

                if (!used_fk_names.contains(foreign_key.fk_name))
                    used_fk_names.insert(foreign_key.fk_name);
                else
                    throw Exception("FOREIGN KEY constraint name duplicated with " + foreign_key.fk_name, ErrorCodes::ILLEGAL_COLUMN);


                auto check_res = contains_columns(foreign_key.column_names->as<ASTExpressionList &>(), columns);
                if (!check_res.empty())
                    throw Exception("FOREIGN KEY references unknown self column " + check_res, ErrorCodes::ILLEGAL_COLUMN);

                if (!force_create_foreign_key)
                {
                    auto ref_storage_ptr = DatabaseCatalog::instance().tryGetTable({create.database, foreign_key.ref_table_name}, getContext());
                    if (!ref_storage_ptr)
                        throw Exception("FOREIGN KEY references unknown table " + foreign_key.ref_table_name, ErrorCodes::UNKNOWN_TABLE);

                    auto ref_check_res = contains_columns(
                        foreign_key.ref_column_names->as<ASTExpressionList &>(),
                        ref_storage_ptr->getInMemoryMetadataPtr()->getColumns().getAll().getNames());

                    if (!ref_check_res.empty())
                        throw Exception("FOREIGN KEY references unknown column " + ref_check_res, ErrorCodes::ILLEGAL_COLUMN);
                }
            }
        }
    }

    // Make sure names in unique not enforced exist.
    if (create.columns_list && create.columns_list->unique && !create.columns_list->unique->children.empty())
    {
        if (create.database.empty())
            create.database = getContext()->getCurrentDatabase();

        auto contains_columns = [](const ASTExpressionList & name_list, const Names & names) {
            NameSet name_set;
            for (const auto & name : names)
                name_set.insert(name);

            for (const auto & ptr : name_list.children)
                if (!name_set.contains(ptr->as<ASTIdentifier &>().name()))
                    return ptr->as<ASTIdentifier &>().name();
            return String();
        };

        Names columns;
        for (const auto & expr : create.columns_list->columns->as<ASTExpressionList &>().children)
            columns.push_back(expr->as<ASTColumnDeclaration &>().name);

        if (!columns.empty())
        {
            const ASTExpressionList * unique = create.columns_list->unique;
            NameSet used_uk_names;

            for (const auto & unique_child : unique->children)
            {
                auto & unique_key = unique_child->as<ASTUniqueNotEnforcedDeclaration &>();

                if (!used_uk_names.contains(unique_key.name))
                    used_uk_names.insert(unique_key.name);
                else
                    throw Exception("UNIQUE NOT ENFORCED constraint name duplicated with " + unique_key.name, ErrorCodes::ILLEGAL_COLUMN);

                auto check_res = contains_columns(unique_key.column_names->as<ASTExpressionList &>(), columns);
                if (!check_res.empty())
                    throw Exception("UNIQUE NOT ENFORCED not exists -- " + check_res, ErrorCodes::ILLEGAL_COLUMN);
            }
        }
    }


    DatabasePtr database;
    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
    {
        database = DatabaseCatalog::instance().tryGetDatabase(database_name, getContext());
        if (!database)
        {
            if (getContext()->getServerType() != ServerType::cnch_worker)
                throw Exception("No database " + database_name + " found", ErrorCodes::UNKNOWN_DATABASE);

            ASTCreateQuery create_database;
            create_database.database = database_name;
            create_database.if_not_exists = true;

            createDatabase(create_database);
            database = DatabaseCatalog::instance().getDatabase(database_name, getContext());
        }
    }

    if (need_add_to_database && database->getEngineName() == "Replicated")
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(create.database, create.table);

        if (auto * ptr = typeid_cast<DatabaseReplicated *>(database.get());
            ptr && !getContext()->getClientInfo().is_replicated_database_internal)
        {
            assertOrSetUUID(create, database);
            guard->releaseTableLock();
            return ptr->tryEnqueueReplicatedDDL(query_ptr, getContext());
        }
    }

    if (need_add_to_database && database->getEngineName() == "Cnch")
    {
        if (!create.cluster.empty())
        {
            throw Exception("Cluster is not supported in Cnch.", ErrorCodes::NOT_IMPLEMENTED);
        }

        assertOrSetUUID(create, database);
    }

    /// CnchKafka table should be allowed to be created on worker
    if (create.storage && startsWith(create.storage->engine->name, "Cnch")
        && !startsWith(create.storage->engine->name, "CnchKafka") && database->getEngineName() != "Cnch" && database->getEngineName() != "CnchMaterializedMySQL")
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Table engine {} is not allowed to create in database engine {}",
            create.storage->engine->name,
            database->getEngineName()
        );
    }

    if (create.replace_table)
        return doCreateOrReplaceTable(create, properties);

    /// when create materialized view and tenant id is not empty add setting tenant_id to select query
    if (create.is_materialized_view && !getCurrentTenantId().empty())
    {
        ASTPtr settings = std::make_shared<ASTSetQuery>();
        settings->as<ASTSetQuery &>().is_standalone = false;
        settings->as<ASTSetQuery &>().changes.push_back({"tenant_id", getCurrentTenantId()});
        ASTPtr select = create.select->clone();
        if (select && select->as<ASTSelectWithUnionQuery &>().settings_ast)
        {
            if (!select->as<ASTSelectWithUnionQuery &>().settings_ast->as<ASTSetQuery &>().changes.tryGet("tenant_id"))
            {
                settings->as<ASTSetQuery &>().changes.merge(select->as<ASTSelectWithUnionQuery &>().settings_ast->as<ASTSetQuery &>().changes);
                ASTSetQuery * setting_ptr = select->as<ASTSelectWithUnionQuery &>().settings_ast->as<ASTSetQuery>();
                select->setOrReplace(setting_ptr, settings);
                create.setOrReplace(create.select, select);
            }
        }
        else
        {
            select->as<ASTSelectWithUnionQuery &>().settings_ast = settings;
            select->children.push_back(settings);
            create.setOrReplace(create.select, select);
        }
    }

    /// Actually creates table
    bool created = doCreateTable(create, properties);

    if (!created)   /// Table already exists
        return {};

    return fillTableIfNeeded(create);
}

bool InterpreterCreateQuery::doCreateTable(ASTCreateQuery & create,
                                           const InterpreterCreateQuery::TableProperties & properties)
{
    std::unique_ptr<DDLGuard> guard;

    String data_path;
    DatabasePtr database;
    IntentLockPtr db_lock;
    IntentLockPtr tb_lock;


    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
    {
        /** If the request specifies IF NOT EXISTS, we allow concurrent CREATE queries (which do nothing).
          * If table doesn't exist, one thread is creating table, while others wait in DDLGuard.
          */
        guard = DatabaseCatalog::instance().getDDLGuard(create.database, create.table);

        database = DatabaseCatalog::instance().getDatabase(create.database, getContext());
        if (database->getEngineName().starts_with("Cnch"))
        {
            auto txn = getContext()->getCurrentTransaction();
            if (!txn)
                throw Exception("Transaction not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);
                        /// May need to acquire kv lock before creating entry
            if (getContext()->getSettingsRef().bypass_ddl_db_lock)
            {
                tb_lock = txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, database->getDatabaseName(), create.table);
                tb_lock->lock();
            }
            else
            {
                db_lock = txn->createIntentLock(IntentLock::DB_LOCK_PREFIX, database->getDatabaseName());
                tb_lock = txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, database->getDatabaseName(), create.table);
                std::lock(*db_lock, *tb_lock);
            }
        }
        assertOrSetUUID(create, database);

        String storage_name = create.is_dictionary ? "Dictionary" : "Table";
        auto storage_already_exists_error_code = create.is_dictionary ? ErrorCodes::DICTIONARY_ALREADY_EXISTS : ErrorCodes::TABLE_ALREADY_EXISTS;

        /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
        if (database->isTableExist(create.table, getContext()))
        {
            /// TODO Check structure of table
            if (create.if_not_exists)
                return false;
            else if (create.replace_view)
            {
                /// when executing CREATE OR REPLACE VIEW, drop current existing view
                auto drop_ast = std::make_shared<ASTDropQuery>();
                drop_ast->database = create.database;
                drop_ast->table = create.table;
                drop_ast->no_ddl_lock = true;

                auto drop_context = Context::createCopy(context);
                InterpreterDropQuery interpreter(drop_ast, drop_context);
                interpreter.execute();
            }
            else
                throw Exception(storage_already_exists_error_code,
                    "{} {}.{} already exists.", storage_name, backQuoteIfNeed(create.database), backQuoteIfNeed(create.table));
        }

        data_path = database->getTableDataPath(create);

        if (!create.attach && !data_path.empty() && fs::exists(fs::path{getContext()->getPath()} / data_path))
            throw Exception(storage_already_exists_error_code,
                "Directory for {} data {} already exists", Poco::toLower(storage_name), String(data_path));
    }
    else
    {
        if (create.if_not_exists && getContext()->tryResolveStorageID({"", create.table}, Context::ResolveExternal))
            return false;

        String temporary_table_name = create.table;
        auto temporary_table = TemporaryTableHolder(getContext(), properties.columns, properties.constraints, properties.foreign_keys, properties.unique, query_ptr);
        getContext()->getSessionContext()->addExternalTable(temporary_table_name, std::move(temporary_table));
        return true;
    }

    bool from_path = create.attach_from_path.has_value();
    String actual_data_path = data_path;
    if (from_path)
    {
        if (data_path.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "ATTACH ... FROM ... query is not supported for {} database engine", database->getEngineName());
        /// We will try to create Storage instance with provided data path
        data_path = *create.attach_from_path;
        create.attach_from_path = std::nullopt;
    }

    if (create.attach)
    {
        /// If table was detached it's not possible to attach it back while some threads are using
        /// old instance of the storage. For example, AsynchronousMetrics may cause ATTACH to fail,
        /// so we allow waiting here. If database_atomic_wait_for_drop_and_detach_synchronously is disabled
        /// and old storage instance still exists it will throw exception.
        bool throw_if_table_in_use = getContext()->getSettingsRef().database_atomic_wait_for_drop_and_detach_synchronously;
        if (throw_if_table_in_use)
            database->checkDetachedTableNotInUse(create.uuid);
        else
            database->waitDetachedTableNotInUse(create.uuid);
    }

    StoragePtr res;
    /// NOTE: CREATE query may be rewritten by Storage creator or table function
    if (create.as_table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        auto table_func = factory.get(create.as_table_function, getContext());
        res = table_func->execute(create.as_table_function, getContext(), create.table, properties.columns);
        res->renameInMemory({create.database, create.table, create.uuid});
    }
    else
    {
        res = StorageFactory::instance().get(create,
            data_path,
            getContext(),
            getContext()->getGlobalContext(),
            properties.columns,
            properties.constraints,
            properties.foreign_keys,
            properties.unique,
            false);
    }

    try
    {
        res->checkColumnsValidity(properties.columns);
    }
    catch (...)
    {
        /// remove directory
        res->drop();
        throw;
    }

    if (from_path && !res->storesDataOnDisk())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "ATTACH ... FROM ... query is not supported for {} table engine, "
                        "because such tables do not store any data on disk. Use CREATE instead.", res->getName());

    database->createTable(getContext(), create.table, res, query_ptr);

    /// Move table data to the proper place. Wo do not move data earlier to avoid situations
    /// when data directory moved, but table has not been created due to some error.
    if (from_path)
        res->rename(actual_data_path, {create.database, create.table, create.uuid});

    res->setUpdateTimeNow();

    /// We must call "startup" and "shutdown" while holding DDLGuard.
    /// Because otherwise method "shutdown" (from InterpreterDropQuery) can be called before startup
    /// (in case when table was created and instantly dropped before started up)
    ///
    /// Method "startup" may create background tasks and method "shutdown" will wait for them.
    /// But if "shutdown" is called before "startup", it will exit early, because there are no background tasks to wait.
    /// Then background task is created by "startup" method. And when destructor of a table object is called, background task is still active,
    /// and the task will use references to freed data.

    /// Also note that "startup" method is exception-safe. If exception is thrown from "startup",
    /// we can safely destroy the object without a call to "shutdown", because there is guarantee
    /// that no background threads/similar resources remain after exception from "startup".

    if (!res->supportsDynamicSubcolumns() && res->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Cannot create table with column of type Object, "
            "because storage {} doesn't support dynamic subcolumns",
            res->getName());
    }

    res->startup();
    return true;
}


BlockIO InterpreterCreateQuery::doCreateOrReplaceTable(ASTCreateQuery & create,
                                                       const InterpreterCreateQuery::TableProperties & properties)
{
    auto ast_drop = std::make_shared<ASTDropQuery>();
    String table_to_replace_name = create.table;
    bool created = false;
    bool replaced = false;

    try
    {
        [[maybe_unused]] bool done = doCreateTable(create, properties);
        assert(done);
        ast_drop->table = create.table;
        ast_drop->is_dictionary = create.is_dictionary;
        ast_drop->database = create.database;
        ast_drop->kind = ASTDropQuery::Drop;
        created = true;
        if (!create.replace_table)
            return fillTableIfNeeded(create);

        auto ast_rename = std::make_shared<ASTRenameQuery>();
        ASTRenameQuery::Element elem
        {
            ASTRenameQuery::Table{create.database, create.table},
            ASTRenameQuery::Table{create.database, table_to_replace_name}
        };

        ast_rename->elements.push_back(std::move(elem));
        ast_rename->exchange = true;
        ast_rename->dictionary = create.is_dictionary;

        InterpreterRenameQuery(ast_rename, getContext()).execute();
        replaced = true;

        InterpreterDropQuery(ast_drop, getContext()).execute();

        create.table = table_to_replace_name;

        return fillTableIfNeeded(create);
    }
    catch (...)
    {
        if (created && create.replace_table && !replaced)
            InterpreterDropQuery(ast_drop, getContext()).execute();
        throw;
    }
}

BlockIO InterpreterCreateQuery::fillTableIfNeeded(const ASTCreateQuery & create)
{
    /// If the query is a CREATE SELECT, insert the data into the table via INSERT INTO ... SELECT FROM
    if (create.select && !create.attach && !create.is_ordinary_view && !create.is_live_view
        && (!create.is_materialized_view || create.is_populate))
    {
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = {create.database, create.table, create.uuid};
        insert->select = create.select->clone();

        if (create.temporary)
        {
            if (!getContext()->getSessionContext()->hasQueryContext())
                getContext()->getSessionContext()->makeQueryContext();

            return InterpreterInsertQuery(insert,
                create.temporary ? getContext()->getSessionContext() : getContext(),
                getContext()->getSettingsRef().insert_allow_materialized_columns).execute();
        }
        else
        {
            /// reuse the query context for INSERT instead of creating a new context,
            /// because we want the outermost executeQuery to finish the INSERT txn rather than the DDL txn
            auto insert_context = getContext()->getQueryContext();
            auto & coordinator = insert_context->getCnchTransactionCoordinator();
            if (insert_context->getCurrentTransaction())
            {
                /// finish the last txn (for DDL) and create a new one for INSERT
                insert_context->setCurrentTransaction(coordinator.createTransaction());
            }

            bool is_internal = true;
            // TODO @wangtao.2077: review this when internal queries are fully supported by optimizer
            if (insert_context->getSettingsRef().enable_optimizer && insert_context->getSettingsRef().enable_optimizer_for_create_select)
            {
                /// optimizer doesn't support internal query
                is_internal = false;
                /// in order to add the insert query to processlist, need to allocate a new query id
                insert_context->setCurrentQueryId("");
            }

            return executeQuery(insert->formatForErrorMessage(), insert_context, is_internal);
        }
    }

    return {};
}

void InterpreterCreateQuery::prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr local_context, const String & cluster_name)
{
    if (create.attach)
        return;

    /// For CREATE query generate UUID on initiator, so it will be the same on all hosts.
    /// It will be ignored if database does not support UUIDs.
    generateUUIDForTable(create);

    /// For cross-replication cluster we cannot use UUID in replica path.
    String cluster_name_expanded = local_context->getMacros()->expand(cluster_name);
    ClusterPtr cluster = local_context->getCluster(cluster_name_expanded);

    if (cluster->maybeCrossReplication())
    {
        /// Check that {uuid} macro is not used in zookeeper_path for ReplicatedMergeTree.
        /// Otherwise replicas will generate different paths.
        if (!create.storage)
            return;
        if (!create.storage->engine)
            return;
        if (!startsWith(create.storage->engine->name, "Replicated"))
            return;

        bool has_explicit_zk_path_arg = create.storage->engine->arguments &&
                                        create.storage->engine->arguments->children.size() >= 2 &&
                                        create.storage->engine->arguments->children[0]->as<ASTLiteral>() &&
                                        create.storage->engine->arguments->children[0]->as<ASTLiteral>()->value.getType() == Field::Types::String;

        if (has_explicit_zk_path_arg)
        {
            String zk_path = create.storage->engine->arguments->children[0]->as<ASTLiteral>()->value.get<String>();
            Macros::MacroExpansionInfo info;
            info.table_id.uuid = create.uuid;
            info.ignore_unknown = true;
            local_context->getMacros()->expand(zk_path, info);
            if (!info.expanded_uuid)
                return;
        }

        throw Exception("Seems like cluster is configured for cross-replication, "
                        "but zookeeper_path for ReplicatedMergeTree is not specified or contains {uuid} macro. "
                        "It's not supported for cross replication, because tables must have different UUIDs. "
                        "Please specify unique zookeeper_path explicitly.", ErrorCodes::INCORRECT_QUERY);
    }
}

BlockIO InterpreterCreateQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create = query_ptr->as<ASTCreateQuery &>();
    if (!create.cluster.empty())
    {
        prepareOnClusterQuery(create, getContext(), create.cluster);
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccess());
    }

    getContext()->checkAccess(getRequiredAccess());

    ASTQueryWithOutput::resetOutputASTIfExist(create);

    if (!create.catalog.empty() && create.database.empty() && create.table.empty())
        return createExternalCatalog(create);
    else if (!create.catalog.empty() && (!create.database.empty() || !create.table.empty()))
        throw Exception("create database or table in externcal catalog is not supported", ErrorCodes::INCORRECT_QUERY );
    /// CREATE|ATTACH DATABASE
    else if (!create.database.empty() && create.table.empty())
        return createDatabase(create);
    else
        return createTable(create);
}


AccessRightsElements InterpreterCreateQuery::getRequiredAccess() const
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return {};

    AccessRightsElements required_access;
    const auto & create = query_ptr->as<const ASTCreateQuery &>();

    if (create.table.empty())
    {
        required_access.emplace_back(AccessType::CREATE_DATABASE, create.database);
    }
    else if (create.is_dictionary)
    {
        required_access.emplace_back(AccessType::CREATE_DICTIONARY, create.database, create.table);
    }
    else if (create.isView())
    {
        assert(!create.temporary);
        if (create.replace_view)
            required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, create.database, create.table);
        else
            required_access.emplace_back(AccessType::CREATE_VIEW, create.database, create.table);
    }
    else
    {
        if (create.temporary)
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
        else
        {
            if (create.replace_table)
                required_access.emplace_back(AccessType::DROP_TABLE, create.database, create.table);
            required_access.emplace_back(AccessType::CREATE_TABLE, create.database, create.table);
        }
    }

    if (create.to_table_id)
        required_access.emplace_back(AccessType::SELECT | AccessType::INSERT, create.to_table_id.database_name, create.to_table_id.table_name);

    if (create.storage && create.storage->engine)
    {
        auto source_access_type = StorageFactory::instance().getSourceAccessType(create.storage->engine->name);
        if (source_access_type != AccessType::NONE)
            required_access.emplace_back(source_access_type);
    }

    return required_access;
}

void InterpreterCreateQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Create";
    if (!as_table_saved.empty())
    {
        String database = backQuoteIfNeed(as_database_saved.empty() ? getContext()->getCurrentDatabase() : as_database_saved);
        elem.query_databases.insert(database);
        elem.query_tables.insert(database + "." + backQuoteIfNeed(as_table_saved));
    }
}

size_t InterpreterCreateQuery::processIgnoreBitEngineEncode(ColumnsDescription & columns) const
{
    size_t changed_column_cnt{0};
    for (const auto & column : columns)
    {
        if (column.type->isBitEngineEncode())
        {
            auto bitmap_type = std::make_shared<DataTypeBitMap64>();
            bitmap_type->setFlags(column.type->getFlags());
            bitmap_type->resetFlags(TYPE_BITENGINE_ENCODE_FLAG);
            const_cast<ColumnDescription &>(column).type = std::move(bitmap_type);
            ++changed_column_cnt;
        }
    }

    return changed_column_cnt;
}

ASTPtr convertMergeTreeToCnchEngine(ASTPtr query_ptr)
{
    auto & create = query_ptr->as<ASTCreateQuery &>();
    if (create.attach)
        return query_ptr;
    ASTStorage * storage = create.storage;
    if (!storage)
        return query_ptr;
    ASTFunction * engine = storage->engine;
    if (!engine)
        return query_ptr;
    const String & engine_name = engine->name;
    bool found_cnch = (engine_name.find("Cnch") != std::string::npos);
    bool found_merge_tree = (engine_name.find("MergeTree") != std::string::npos);
    if (found_cnch || (!found_merge_tree))
        return query_ptr;

    String new_engine_name = "Cnch" + engine_name;
    engine->name = std::move(new_engine_name);
    return query_ptr;
}

}
