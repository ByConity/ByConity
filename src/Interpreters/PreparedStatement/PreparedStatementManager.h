#pragma once

#include <Analyzers/Analysis.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedStatement/PreparedStatementLoaderFromDisk.h>
#include <Interpreters/prepared_statement.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#include <Common/SettingsChanges.h>

#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace DB
{
namespace Protos
{
    class PreparedStatement;
}

using NamesAndPreparedStatements = std::vector<std::pair<String, Protos::PreparedStatement>>;

struct PreparedObject
{
    struct QueryAccessInfo
    {
        // database_name->table_name->column_names
        std::unordered_map<String, std::unordered_map<String, std::vector<String>>> query_access_info;
    };

    String query;
    SettingsChanges settings_changes;
    PreparedParameterSet prepared_params;
    std::shared_ptr<QueryAccessInfo> query_detail;

    PlanNodePtr plan_root;
    std::unordered_map<CTEId, PlanNodePtr> cte_map;

    void toProto(Protos::PreparedStatement & proto) const;
};

class PreparedStatementManager
{
public:
    using CacheType = std::unordered_map<String, PreparedObject>;

    static void initialize(ContextMutablePtr context);

    void
    set(const String & name,
        PreparedObject prepared_object,
        bool throw_if_exists = true,
        bool or_replace = false,
        bool is_persistent = true);
    PreparedObject getObject(const String & name) const;
    SettingsChanges getSettings(const String & name) const;
    void remove(const String & name, bool throw_if_not_exists);
    void clearCache();
    Strings getNames() const;
    bool has(const String & name) const;
    NamesAndPreparedStatements getAllStatementsFromDisk(ContextMutablePtr & context);

    struct CacheResultType
    {
        QueryPlanPtr plan;
        PreparedParameterSet prepared_params;
    };

    // TODO @wangtao: extract common logic with InterpreterSelectQueryUseOptimizer::getPlanFromCache
    CacheResultType getPlanFromCache(const String & name, ContextMutablePtr & context) const;
    // TODO @wangtao: extract common logic with InterpreterSelectQueryUseOptimizer::addPlanToCache
    void addPlanToCache(
        const String & name,
        const String & query,
        SettingsChanges settings_changes,
        QueryPlanPtr & plan,
        AnalysisPtr analysis,
        PreparedParameterSet prepared_params,
        ContextMutablePtr & context,
        bool throw_if_exists,
        bool or_replace,
        bool is_persistent);

    static void loadStatementsFromDisk(ContextMutablePtr & context);

private:
    CacheType cache;
    mutable std::shared_mutex mutex;

    std::unique_ptr<PreparedStatementLoaderFromDisk> prepared_statement_loader;

    bool hasUnsafe(const String & name) const
    {
        return cache.contains(name);
    }

    // TODO @wangtao: extract common logic with PlanCache::getNewPlanNode
    static PlanNodePtr getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id);
    const PreparedObject & getUnsafe(const String & name) const;
};


}
