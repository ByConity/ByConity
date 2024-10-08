#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ParserPreparedStatement.h>
#include <Parsers/parseQuery.h>
#include <Protos/plan_node.pb.h>
#include <Interpreters/StorageID.h>
#include "Interpreters/PreparedStatement/PreparedStatementCatalog.h"
#include "Parsers/IAST_fwd.h"

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int PREPARED_STATEMENT_ALREADY_EXISTS;
    extern const int PREPARED_STATEMENT_NOT_EXISTS;
}
void PreparedObject::toProto(Protos::PreparedStatement & proto) const
{
    proto.set_query(query->formatForErrorMessage());
}

void PreparedStatementManager::initialize(ContextMutablePtr context)
{
    if (!context->getPreparedStatementManager())
    {
        auto manager_instance = std::make_unique<PreparedStatementManager>();
        context->setPreparedStatementManager(std::move(manager_instance));
        loadStatementsFromCatalog(context);
    }
}

void PreparedStatementManager::set(
    const String & name, PreparedObject prepared_object, bool throw_if_exists, bool or_replace, bool is_persistent, ContextMutablePtr context)
{
    std::unique_lock lock(mutex);

    if (!hasUnsafe(name) || or_replace)
    {
        if (is_persistent)
        {
            if (!context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Context is nullptr");
            Protos::PreparedStatement proto;
            PreparedStatementCatalogManager catalog(context);
            PreparedStatementItemPtr prepared = std::make_shared<PreparedStatementItem>(name, prepared_object.query->formatForErrorMessage());
            catalog.updatePreparedStatement(prepared);
        }
        cache[name] = std::move(prepared_object);
    }
    else if (throw_if_exists)
        throw Exception(ErrorCodes::PREPARED_STATEMENT_ALREADY_EXISTS, "Prepared statement already exists");
}

PreparedObject PreparedStatementManager::getObject(const String & name) const
{
    std::shared_lock lock(mutex);
    return getUnsafe(name);
}

SettingsChanges PreparedStatementManager::getSettings(const String & name) const
{
    std::shared_lock lock(mutex);
    return getUnsafe(name).settings_changes;
}

void PreparedStatementManager::remove(const String & name, bool throw_if_not_exists, ContextMutablePtr context)
{
    std::unique_lock lock(mutex);
    if (context)
    {
        PreparedStatementCatalogManager catalog(context);
        catalog.removePreparedStatement(name);
    }

    if (hasUnsafe(name))
        cache.erase(name);
    else if (throw_if_not_exists)
        throw Exception(ErrorCodes::PREPARED_STATEMENT_NOT_EXISTS, "Prepared statement not exists");
}

PreparedStatementManager::CacheResultType PreparedStatementManager::getPlanFromCache(const String & name, ContextMutablePtr & context) const
{
    std::shared_lock lock(mutex);
    const auto & prepared_object = getUnsafe(name);

    PlanNodeId max_id;
    auto root = getNewPlanNode(prepared_object.plan_root, context, false, max_id);
    CTEInfo cte_info;
    for (const auto & cte : prepared_object.cte_map)
        cte_info.add(cte.first, getNewPlanNode(cte.second, context, false, max_id));

    if (prepared_object.query_detail && context->hasQueryContext())
    {
        for (auto & [database, table_info] : prepared_object.query_detail->query_access_info)
        {
            for (auto & [table, columns] : table_info)
            {
                auto storage_id = context->tryResolveStorageID(StorageID{database, table});
                context->checkAccess(AccessType::SELECT, storage_id, columns);
                context->addQueryAccessInfo(backQuoteIfNeed(storage_id.getDatabaseName()), storage_id.getFullTableName(), columns);
            }   
        }
    }

    auto node_id_allocator = std::make_shared<PlanNodeIdAllocator>(max_id + 1);
    auto query_plan = std::make_unique<QueryPlan>(root, cte_info, node_id_allocator);
    return {.plan = std::move(query_plan), .prepared_params = prepared_object.prepared_params};
}

void PreparedStatementManager::addPlanToCache(
    const String & name,
    ASTPtr & query,
    SettingsChanges settings_changes,
    QueryPlanPtr & plan,
    AnalysisPtr analysis,
    PreparedParameterSet prepared_params,
    ContextMutablePtr & context)
{
    PlanNodeId max_id;
    PreparedObject prepared_object{};
    prepared_object.query = query;
    prepared_object.settings_changes = std::move(settings_changes);
    prepared_object.prepared_params = std::move(prepared_params);
    prepared_object.plan_root = getNewPlanNode(plan->getPlanNode(), context, true, max_id);

    for (const auto & cte : plan->getCTEInfo().getCTEs())
        prepared_object.cte_map.emplace(cte.first, getNewPlanNode(cte.second, context, true, max_id));

    prepared_object.query_detail = std::make_shared<PreparedObject::QueryAccessInfo>();
    const auto & used_columns_map = analysis->getUsedColumns();
    for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
    {
        if (!storage_analysis.storage)
            continue;
        auto storage_id = storage_analysis.storage->getStorageID();
        if (auto it = used_columns_map.find(storage_analysis.storage->getStorageID()); it != used_columns_map.end())
        {
            for (const auto & column : it->second)
                prepared_object.query_detail
                    ->query_access_info[storage_id.getDatabaseName()][storage_id.getTableName()]
                    .emplace_back(column);
        }
    }
    const auto & prepare = query->as<const ASTCreatePreparedStatementQuery &>();
    set(name, std::move(prepared_object), !prepare.if_not_exists, prepare.or_replace, prepare.is_permanent, context);
}

PlanNodePtr PreparedStatementManager::getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id)
{
    if (max_id < node->getId())
        max_id = node->getId();

    if (node->getType() == IQueryPlanStep::Type::TableScan)
    {
        auto step = node->getStep()->copy(context);
        auto * table_step = dynamic_cast<TableScanStep *>(step.get());
        if (cache_plan)
            table_step->cleanStorage();
        else
            table_step->setStorage(context);
        return PlanNodeBase::createPlanNode(node->getId(), step, {});
    }

    PlanNodes children;
    for (auto & child : node->getChildren())
    {
        auto result_node = getNewPlanNode(child, context, cache_plan, max_id);
        if (result_node)
            children.emplace_back(result_node);
    }

    return PlanNodeBase::createPlanNode(node->getId(), node->getStep()->copy(context), children);
}

const PreparedObject & PreparedStatementManager::getUnsafe(const String & name) const
{
    auto it = cache.find(name);
    if (it == cache.end())
        throw Exception(ErrorCodes::PREPARED_STATEMENT_NOT_EXISTS, "Prepared statement not exists");

    return it->second;
}

Strings PreparedStatementManager::getNames() const
{
    std::shared_lock lock(mutex);
    Strings res;
    res.reserve(cache.size());

    for (const auto & elem : cache)
        res.push_back(elem.first);

    return res;
}

bool PreparedStatementManager::has(const String & name) const
{
    std::shared_lock lock(mutex);
    bool contains = hasUnsafe(name);
    return contains;
}

void PreparedStatementManager::clearCache()
{
    std::unique_lock lock(mutex);
    cache.clear();
}

void PreparedStatementManager::loadStatementsFromCatalog(ContextMutablePtr & context)
{
    if (!context->getPreparedStatementManager())
        throw Exception("PreparedStatement cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    auto * manager = context->getPreparedStatementManager();
    manager->clearCache();
    PreparedStatementCatalogManager catalog(context);
    auto statements = catalog.getPreparedStatements();
    for (auto & statement : statements)
    {
        try
        {
            ParserCreatePreparedStatementQuery parser(ParserSettings::valueOf(context->getSettingsRef()));
            auto ast = parseQuery(parser, statement->create_statement, "", 0, context->getSettings().max_parser_depth);
            auto * create_prep_stat = ast->as<ASTCreatePreparedStatementQuery>();

            if (!create_prep_stat)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid prepread statement query: {}", statement->create_statement);

            create_prep_stat->is_permanent = false;
            auto query_context = Context::createCopy(context);
            query_context->setQueryContext(query_context);
            SettingsChanges settings_changes = InterpreterSetQuery::extractSettingsFromQuery(ast, query_context);
            query_context->applySettingsChanges(settings_changes);
            InterpreterSelectQueryUseOptimizer interpreter{ast, query_context, {}};
            interpreter.executeCreatePreparedStatementQuery();
        }
        catch (...)
        {
            tryLogWarningCurrentException(
                getLogger("PreparedStatementManager"),
                fmt::format("while build prepared statement {} plan", backQuote(statement->name)));
            continue;
        }
    }
}

}
