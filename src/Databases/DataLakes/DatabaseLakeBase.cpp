#include "DatabaseLakeBase.h"

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageFactory.h>


namespace DB
{
DatabaseLakeBase::DatabaseLakeBase(const String & database_name_, const String & metadata_path_, const ASTStorage * database_engine_define_)
    : IDatabase(database_name_), metadata_path(metadata_path_), database_engine_define(database_engine_define_->clone())
{
}

ASTPtr DatabaseLakeBase::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = getDatabaseName();
    create_query->set(create_query->storage, database_engine_define);

    addCreateQuerySettings(*create_query, storage_settings);

    return create_query;
}

StoragePtr DatabaseLakeBase::tryGetTable(const String & table_name, ContextPtr local_context) const
{
    auto ast = getCreateTableQueryImpl(table_name, local_context, false);
    if (!ast)
    {
        return nullptr;
    }
    const auto & create_query_ast = ast->as<ASTCreateQuery &>();
    auto create_query = serializeAST(create_query_ast);
    LOG_TRACE(log, "{}.{} create query: {}", getDatabaseName(), table_name, create_query);
    ContextMutablePtr mutable_context = Context::createCopy(local_context);
    auto ret = StorageFactory::instance().get(
        create_query_ast,
        "",
        mutable_context,
        mutable_context->getGlobalContext(),
        InterpreterCreateQuery::getColumnsDescription(*create_query_ast.columns_list->columns, mutable_context, false),
        InterpreterCreateQuery::getConstraintsDescription(create_query_ast.columns_list->constraints),
        {},
        {},
        false);
    auto metadata = ret->getInMemoryMetadataCopy();
    ret->setInMemoryMetadata(metadata);
    return ret;
}

void DatabaseLakeBase::addCreateQuerySettings(ASTCreateQuery & create_query, const CnchHiveSettingsPtr & storage_settings)
{
    if (!create_query.storage)
        return;
    ASTSetQuery * set_query = create_query.storage->settings;
    if (set_query == nullptr)
    {
        create_query.storage->set(create_query.storage->settings, std::make_shared<ASTSetQuery>());
        set_query = create_query.storage->settings;
    }
    set_query->is_standalone = false;
    set_query->changes = storage_settings->changes();
}
}
