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

    DistributedStagesSettings distributed_stages_settings = InterpreterDistributedStages::extractDistributedStagesSettings(query, context);

    bool use_distributed_stages = (distributed_stages_settings.enable_distributed_stages) && !options.is_internal;

    if (use_distributed_stages)
    {
        if (!context->getComplexQueryActive())
            throw Exception("Server config missing exchange_status_port and exchange_port cannot execute query with enable_distributed_stages enabled",
                            ErrorCodes::LOGICAL_ERROR);

        if (query->as<ASTSelectQuery>() || query->as<ASTSelectWithUnionQuery>())
            return std::make_unique<InterpreterDistributedStages>(query, context);
    }

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
        if (QueryUseOptimizerChecker::check(query, context)) {
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
        }
        ProfileEvents::increment(ProfileEvents::SelectQuery);
        return std::make_unique<InterpreterSelectWithUnionQuery>(query, context, options);
    }
    else if (query->as<ASTSelectIntersectExceptQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context)) {
            return std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context, options);
        }
        throw Exception("Intersect & except requires optimizer enabled.(SET enable_optimizer=1)", ErrorCodes::NOT_IMPLEMENTED);
    }
    else if (query->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::InsertQuery);
        bool allow_materialized = static_cast<bool>(context->getSettingsRef().insert_allow_materialized_columns);
        return std::make_unique<InterpreterInsertQuery>(query, context, allow_materialized);
    }
    else if (query->as<ASTUpdateQuery>())
    {
        return std::make_unique<InterpreterUpdateQuery>(query, context);
    }
    else if (query->as<ASTCreateQuery>())
    {
        return std::make_unique<InterpreterCreateQuery>(query, context);
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
    else if (query->as<ASTUseQuery>())
    {
        return std::make_unique<InterpreterUseQuery>(query, context);
    }
    else if (query->as<ASTSetQuery>())
    {
        /// readonly is checked inside InterpreterSetQuery
        return std::make_unique<InterpreterSetQuery>(query, context);
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
    else if (query->as<ASTAlterQuery>())
    {
        return std::make_unique<InterpreterAlterQuery>(query, context);
    }
    else if (query->as<ASTCheckQuery>())
    {
        return std::make_unique<InterpreterCheckQuery>(query, context);
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
    else if (query->as<ASTDropStatsQuery>())
    {
        return std::make_unique<InterpreterDropStatsQuery>(query, context);
    }
    else if (query->as<ASTShowStatsQuery>())
    {
        return std::make_unique<InterpreterShowStatsQuery>(query, context);
    }
    else if (query->as<ASTDumpInfoQuery>())
    {
        if (QueryUseOptimizerChecker::check(query, context))
        {
            return std::make_unique<InterpreterDumpInfoQueryUseOptimizer>(query, context);
        }
        else
            throw Exception("Not support dump query, because it's optimizer check fail.", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
    }
    else if (query->as<ASTReproduceQuery>())
    {
        return std::make_unique<InterpreterReproduceQueryUseOptimizer>(query, context);
    }
    else if (query->as<ASTDeleteQuery>())
    {
        return std::make_unique<InterpreterDeleteQuery>(query, context);
    }
    else
    {
        throw Exception("Unknown type of query: " + query->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
    }
}
}
