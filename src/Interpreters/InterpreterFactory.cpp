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

#include <Interpreters/Context.h>
#include <Parsers/ASTAdviseQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateQueryAnalyticalMySQL.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTCreateWarehouseQuery.h>
#include <Parsers/ASTCreateWorkerGroupQuery.h>
#include <Parsers/ASTSQLBinding.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTDropWarehouseQuery.h>
#include <Parsers/ASTDropWorkerGroupQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTReproduceQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSetSensitiveQuery.h>
#include <Parsers/ASTSetRoleQuery.h>
#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Parsers/ASTShowAccessQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/ASTShowPrivilegesQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTShowWarehousesQuery.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTDumpQuery.h>
#include <Parsers/ASTReproduceQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Parsers/ASTSwitchQuery.h>
#include <Parsers/ASTUndropQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTAutoStatsQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/InterpreterDistributedStages.h>

#include <Interpreters/InterpreterAdviseQuery.h>
#include <Interpreters/InterpreterAlterDiskCacheQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterAlterWarehouseQuery.h>
#include <Interpreters/InterpreterBackupQuery.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/InterpreterCreateBindingQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Interpreters/InterpreterCreateSettingsProfileQuery.h>
#include <Interpreters/InterpreterCreateSnapshotQuery.h>
#include <Interpreters/InterpreterCreateStatsQuery.h>
#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/InterpreterCreateWarehouseQuery.h>
#include <Interpreters/InterpreterCreateWorkerGroupQuery.h>
#include <Interpreters/InterpreterDeleteQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterDropAccessEntityQuery.h>
#include <Interpreters/InterpreterDropBindingQuery.h>
#include <Interpreters/InterpreterDropPreparedStatementQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterDropStatsQuery.h>
#include <Interpreters/InterpreterDropWarehouseQuery.h>
#include <Interpreters/InterpreterDropWorkerGroupQuery.h>
#include <Interpreters/InterpreterDumpQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/InterpreterExternalDDLQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Interpreters/InterpreterRefreshQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterReproduceQuery.h>
#include <Interpreters/InterpreterSelectIntersectExceptQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSetSensitiveQuery.h>
#include <Interpreters/InterpreterSetRoleQuery.h>
#include <Interpreters/InterpreterShowAccessEntitiesQuery.h>
#include <Interpreters/InterpreterShowAccessQuery.h>
#include <Interpreters/InterpreterShowBindingsQuery.h>
#include <Interpreters/InterpreterShowColumnsQuery.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Interpreters/InterpreterShowPreparedStatementQuery.h>
#include <Interpreters/InterpreterShowPrivilegesQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>
#include <Interpreters/InterpreterShowSettingQuery.h>
#include <Interpreters/InterpreterShowStatsQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/InterpreterShowWarehousesQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Optimizer/QueryUseOptimizerChecker.h>
#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Interpreters/InterpreterAutoStatsQuery.h>

#include <Interpreters/MySQL/InterpretersAnalyticalMySQLDDLQuery.h>

#include <Parsers/ASTSystemQuery.h>

#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Parsers/ASTExternalDDLQuery.h>
#include "common/logger_useful.h"
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include "Interpreters/DistributedStages/PlanSegment.h"

#include <Interpreters/InterpreterBeginQuery.h>
#include <Interpreters/InterpreterCommitQuery.h>
#include <Interpreters/InterpreterRollbackQuery.h>
#include <Interpreters/InterpreterShowStatementsQuery.h>
#include <Interpreters/InterpreterSwitchQuery.h>
#include <Parsers/ASTTransaction.h>


namespace ProfileEvents
{
    extern const Event Query;
    extern const Event SelectQuery;
    extern const Event InsertQuery;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NOT_IMPLEMENTED;
}


std::unique_ptr<IInterpreter> InterpreterFactory::get(ASTPtr & query, ContextMutablePtr context, const SelectQueryOptions & options)
{
    OpenTelemetrySpanHolder span("InterpreterFactory::get()");

    ProfileEvents::increment(ProfileEvents::Query);

    // in ce, we will disable complex query without optimizer
    /*
    DistributedStagesSettings distributed_stages_settings = InterpreterDistributedStages::extractDistributedStagesSettings(query, context);

    bool use_distributed_stages = (distributed_stages_settings.enable_distributed_stages) && !options.is_internal;
    use_distributed_stages = use_distributed_stages && !context->getSettingsRef().enable_optimizer && PlanSegmentHelper::supportDistributedStages(query);

    if (use_distributed_stages && context->getComplexQueryActive() && QueryUseOptimizerChecker::check(query, context, true))
    {
        if (query->as<ASTSelectQuery>() || query->as<ASTSelectWithUnionQuery>())
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
    }
    */

    if (query->as<ASTSelectQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context)) {
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
        }
        /// This is internal part of ASTSelectWithUnionQuery.
        /// Even if there is SELECT without union, it is represented by ASTSelectWithUnionQuery with single ASTSelectQuery as a child.
        return std::make_unique<InterpreterSelectQuery>(query, context, options);
    }
    else if (query->as<ASTSelectWithUnionQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context))
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);

        ProfileEvents::increment(ProfileEvents::SelectQuery);
        return std::make_unique<InterpreterSelectWithUnionQuery>(query, context, options);
    }
    else if (query->as<ASTSelectIntersectExceptQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context)) {
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
        }
        return std::make_unique<InterpreterSelectIntersectExceptQuery>(query, context, options);
    }
    else if (query->as<ASTInsertQuery>())
    {
        /// currently, the optimizer hasn't support insert select, call the check to force close optimizer.
        QueryUseOptimizerChecker::check(query, context);

        ProfileEvents::increment(ProfileEvents::InsertQuery);
        if (QueryUseOptimizerChecker::check(query, context))
        {
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
        }
        bool allow_materialized = static_cast<bool>(context->getSettingsRef().insert_allow_materialized_columns);
        return std::make_unique<InterpreterInsertQuery>(query, context, allow_materialized);
    }
    else if (query->as<ASTUpdateQuery>())
    {
        return std::make_unique<InterpreterUpdateQuery>(query, context);
    }
    else if (query->as<ASTCreateQueryAnalyticalMySQL>())
    {
        return std::make_unique<MySQLInterpreter::InterpreterAnalyticalMySQLCreateQuery>(query, context);
    }
    else if (query->as<ASTCreateQuery>())
    {
        return std::make_unique<InterpreterCreateQuery>(query, context);
    }
    else if (query->as<ASTCreateSnapshotQuery>())
    {
        return std::make_unique<InterpreterCreateSnapshotQuery>(query, context);
    }
    else if (query->as<ASTDropQuery>())
    {
        return std::make_unique<InterpreterDropQuery>(query, context);
    }
    else if (query->as<ASTUndropQuery>())
    {
        return std::make_unique<InterpreterUndropQuery>(query, context);
    }
    else if (query->as<ASTRenameQuery>())
    {
        return std::make_unique<InterpreterRenameQuery>(query, context);
    }
    else if (query->as<ASTShowTablesQuery>())
    {
        return std::make_unique<InterpreterShowTablesQuery>(query, context);
    }
    else if (query->as<ASTShowColumnsQuery>())
    {
        return std::make_unique<InterpreterShowColumnsQuery>(query, context);
    }
    else if (query->as<ASTUseQuery>())
    {
        return std::make_unique<InterpreterUseQuery>(query, context);
    }
    else if (query->as<ASTShowSettingQuery>())
    {
        return std::make_unique<InterpreterShowSettingQuery>(query, context);
    }
    else if (query->as<ASTSwitchQuery>())
    {
        return std::make_unique<InterpreterSwitchQuery>(query, context);
    }
    else if (query->as<ASTAdviseQuery>())
    {
        return std::make_unique<InterpreterAdviseQuery>(query, context);
    }
    else if (query->as<ASTSetQuery>())
    {
        /// readonly is checked inside InterpreterSetQuery
        return std::make_unique<InterpreterSetQuery>(query, context);
    }
    else if (query->as<ASTSetSensitiveQuery>())
    {
        return std::make_unique<InterpreterSetSensitiveQuery>(query, context);
    }
    else if (query->as<ASTSetRoleQuery>())
    {
        return std::make_unique<InterpreterSetRoleQuery>(query, context);
    }
    else if (query->as<ASTOptimizeQuery>())
    {
        return std::make_unique<InterpreterOptimizeQuery>(query, context);
    }
    else if (query->as<ASTExistsDatabaseQuery>())
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (query->as<ASTExistsTableQuery>())
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (query->as<ASTExistsViewQuery>())
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (query->as<ASTExistsDictionaryQuery>())
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (query->as<ASTShowCreateTableQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTShowCreateViewQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTShowCreateDatabaseQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTShowCreateExternalCatalogQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTShowCreateExternalTableQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTShowCreateDictionaryQuery>())
    {
        return std::make_unique<InterpreterShowCreateQuery>(query, context);
    }
    else if (query->as<ASTDescribeQuery>())
    {
        return std::make_unique<InterpreterDescribeQuery>(query, context);
    }
    else if (query->as<ASTExplainQuery>())
    {
        return std::make_unique<InterpreterExplainQuery>(query, context);
    }
    else if (query->as<ASTShowProcesslistQuery>())
    {
        return std::make_unique<InterpreterShowProcesslistQuery>(query, context);
    }
    else if (query->as<ASTAlterAnalyticalMySQLQuery>())
    {
        return std::make_unique<MySQLInterpreter::InterpreterAnalyticalMySQLAlterQuery>(query, context);
    }
    else if (query->as<ASTAlterQuery>())
    {
        return std::make_unique<InterpreterAlterQuery>(query, context);
    }
    else if (query->as<ASTCheckQuery>())
    {
        return std::make_unique<InterpreterCheckQuery>(query, context);
    }
    else if (query->as<ASTAlterDiskCacheQuery>())
    {
        return std::make_unique<InterpreterAlterDiskCacheQuery>(query, context);
    }
    else if (query->as<ASTKillQueryQuery>())
    {
        return std::make_unique<InterpreterKillQueryQuery>(query, context);
    }
    else if (query->as<ASTSystemQuery>())
    {
        return std::make_unique<InterpreterSystemQuery>(query, context);
    }
    else if (query->as<ASTWatchQuery>())
    {
        return std::make_unique<InterpreterWatchQuery>(query, context);
    }
    else if (query->as<ASTCreateUserQuery>())
    {
        return std::make_unique<InterpreterCreateUserQuery>(query, context);
    }
    else if (query->as<ASTCreateRoleQuery>())
    {
        return std::make_unique<InterpreterCreateRoleQuery>(query, context);
    }
    else if (query->as<ASTCreateQuotaQuery>())
    {
        return std::make_unique<InterpreterCreateQuotaQuery>(query, context);
    }
    else if (query->as<ASTCreateRowPolicyQuery>())
    {
        return std::make_unique<InterpreterCreateRowPolicyQuery>(query, context);
    }
    else if (query->as<ASTCreateSettingsProfileQuery>())
    {
        return std::make_unique<InterpreterCreateSettingsProfileQuery>(query, context);
    }
    else if (query->as<ASTDropAccessEntityQuery>())
    {
        return std::make_unique<InterpreterDropAccessEntityQuery>(query, context);
    }
    else if (query->as<ASTGrantQuery>())
    {
        return std::make_unique<InterpreterGrantQuery>(query, context);
    }
    else if (query->as<ASTShowCreateAccessEntityQuery>())
    {
        return std::make_unique<InterpreterShowCreateAccessEntityQuery>(query, context);
    }
    else if (query->as<ASTShowGrantsQuery>())
    {
        return std::make_unique<InterpreterShowGrantsQuery>(query, context);
    }
    else if (query->as<ASTShowAccessEntitiesQuery>())
    {
        return std::make_unique<InterpreterShowAccessEntitiesQuery>(query, context);
    }
    else if (query->as<ASTShowAccessQuery>())
    {
        return std::make_unique<InterpreterShowAccessQuery>(query, context);
    }
    else if (query->as<ASTShowPrivilegesQuery>())
    {
        return std::make_unique<InterpreterShowPrivilegesQuery>(query, context);
    }
    else if (query->as<ASTExternalDDLQuery>())
    {
        return std::make_unique<InterpreterExternalDDLQuery>(query, context);
    }
    else if (query->as<ASTRefreshQuery>())
    {
        return std::make_unique<InterpreterRefreshQuery>(query, context);
    }
        else if (query->as<ASTAlterWarehouseQuery>())
    {
        return std::make_unique<InterpreterAlterWarehouseQuery>(query, context);
    }
    else if (query->as<ASTCreateWarehouseQuery>())
    {
        return std::make_unique<InterpreterCreateWarehouseQuery>(query, context);
    }
    else if (query->as<ASTDropWarehouseQuery>())
    {
        return std::make_unique<InterpreterDropWarehouseQuery>(query, context);
    }
    else if (query->as<ASTShowWarehousesQuery>())
    {
        return std::make_unique<InterpreterShowWarehousesQuery>(query, context);
    }
    else if (query->as<ASTCreateWorkerGroupQuery>())
    {
        return std::make_unique<InterpreterCreateWorkerGroupQuery>(query, context);
    }
    else if (query->as<ASTDropWorkerGroupQuery>())
    {
        return std::make_unique<InterpreterDropWorkerGroupQuery>(query, context);
    }
    else if (query->as<ASTCreateStatsQuery>())
    {
        return std::make_unique<InterpreterCreateStatsQuery>(query, context);
    }
    else if (query->as<ASTAutoStatsQuery>())
    {
        return std::make_unique<InterpreterAutoStatsQuery>(query, context);
    }
    else if (query->as<ASTDropStatsQuery>())
    {
        return std::make_unique<InterpreterDropStatsQuery>(query, context);
    }
    else if (query->as<ASTShowStatsQuery>())
    {
        return std::make_unique<InterpreterShowStatsQuery>(query, context);
    }
    else if (query->as<ASTDumpQuery>())
    {
        return std::make_unique<InterpreterDumpQuery>(query, context);
    }
    else if (query->as<ASTReproduceQuery>())
    {
        return std::make_unique<InterpreterReproduceQuery>(query, context);
    }
    else if (query->as<ASTBeginQuery>())
    {
        return std::make_unique<InterpreterBeginQuery>(query, context);
    }
    else if (query->as<ASTBeginTransactionQuery>())
    {
        return std::make_unique<InterpreterBeginQuery>(query, context);
    }
    else if (query->as<ASTCommitQuery>())
    {
        return std::make_unique<InterpreterCommitQuery>(query, context);
    }
    else if (query->as<ASTRollbackQuery>())
    {
        return std::make_unique<InterpreterRollbackQuery>(query, context);
    }
    else if (query->as<ASTShowStatementsQuery>())
    {
        return std::make_unique<InterpreterShowStatementsQuery>(query, context);
    }
    else if (query->as<ASTDeleteQuery>())
    {
        return std::make_unique<InterpreterDeleteQuery>(query, context);
    }
    else if (query->as<ASTCreateBinding>())
    {
        return std::make_unique<InterpreterCreateBindingQuery>(query, context);
    }
    else if (query->as<ASTShowBindings>())
    {
        return std::make_unique<InterpreterShowBindingsQuery>(query, context);
    }
    else if (query->as<ASTDropBinding>())
    {
        return std::make_unique<InterpreterDropBindingQuery>(query, context);
    }
    else if (query->as<ASTCreatePreparedStatementQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context, true))
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);

        throw Exception("Prepared statements requires optimizer enabled", ErrorCodes::NOT_IMPLEMENTED);
    }
    else if (query->as<ASTExecutePreparedStatementQuery>())
    {
        if (!context->getSettings().enable_optimizer)
            throw Exception("Execute prepared statements requires optimizer enabled", ErrorCodes::NOT_IMPLEMENTED);
        return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
    }
    else if (query->as<ASTShowPreparedStatementQuery>())
    {
        return std::make_unique<InterpreterShowPreparedStatementQuery>(query, context);
    }
    else if (query->as<ASTDropPreparedStatementQuery>())
    {
        return std::make_unique<InterpreterDropPreparedStatementQuery>(query, context);
    }
    else if (query->as<ASTBackupQuery>())
    {
        return std::make_unique<InterpreterBackupQuery>(query, context);
    }
    else
    {
        throw Exception("Unknown type of query: " + query->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
    }
}
}
