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

#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/SegmentScheduler.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/queryNormalization.h>
#include <common/types.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/LabelledMetrics.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/IBlockInputStream.h>
#include <common/logger_useful.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/DistributedStages/MPPQueryManager.h>
#include <chrono>

namespace ProfileEvents
{
extern const Event UserTimeMicroseconds;
extern const Event SystemTimeMicroseconds;
}

namespace LabelledMetrics
{
extern const Metric UnlimitedQuery;
extern const Metric VwQuery;
}

namespace CurrentMetrics
{
extern const Metric InsertQuery;
extern const Metric SystemQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int QUERY_CPU_TIMEOUT_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
}


/// Should we execute the query even if max_concurrent_queries limit is exhausted
static bool isUnlimitedQuery(const IAST * ast)
{
    if (!ast)
        return false;

    /// It is KILL QUERY
    if (ast->as<ASTKillQueryQuery>())
        return true;

    /// It is SELECT FROM system.processes
    /// NOTE: This is very rough check.
    /// False negative: USE system; SELECT * FROM processes;
    /// False positive: SELECT * FROM system.processes CROSS JOIN (SELECT ...)

    if (const auto * ast_selects = ast->as<ASTSelectWithUnionQuery>())
    {
        if (!ast_selects->list_of_selects || ast_selects->list_of_selects->children.empty())
            return false;

        const auto * ast_select = ast_selects->list_of_selects->children[0]->as<ASTSelectQuery>();
        if (!ast_select)
            return false;

        if (auto database_and_table = getDatabaseAndTable(*ast_select, 0))
            return database_and_table->database == "system" &&
                (database_and_table->table == "processes" || database_and_table->table == "resource_groups");

        return false;
    }

    return false;
}

static bool isMonitoredCnchTable(const String & database)
{
    return database != "system" && database != "cnch_system";
}

static ProcessListQueryType getProcessListQueryType(const IAST * ast)
{
    if (!ast)
        return ProcessListQueryType::Default;

    if (ast->as<ASTInsertQuery>())
        return ProcessListQueryType::Insert;

    if (ast->as<ASTAlterQuery>()) /// XXX: Should we rename ProcessListQueryType::Insert ?
        return ProcessListQueryType::Insert;

    // Check for system SELECT queries
    if (const auto * ast_selects = ast->as<ASTSelectWithUnionQuery>())
    {

        if (!ast_selects->list_of_selects || ast_selects->list_of_selects->children.empty())
            return ProcessListQueryType::Default;

        ASTs all_tables;
        bool dummy = false;
        ASTSelectQuery::collectAllTables(ast_selects, all_tables, dummy);

        for (const auto & table : all_tables)
        {
            auto database_and_table = IdentifierSemantic::extractDatabaseAndTable(typeid_cast<ASTTableIdentifier&>(*table));
                if (isMonitoredCnchTable(database_and_table.first))
                    return ProcessListQueryType::Default;
        }
        if (!all_tables.empty())
            return ProcessListQueryType::System;
    }

    return ProcessListQueryType::Default;
}

static CurrentMetrics::Metric getQueryTypeMetric(ProcessListQueryType & query_type)
{
    if (query_type == ProcessListQueryType::Insert)
    {
        return CurrentMetrics::InsertQuery;
    }
    else if (query_type == ProcessListQueryType::System)
    {
        return CurrentMetrics::SystemQuery;
    }

    return CurrentMetrics::DefaultQuery;
}

static bool isMonitoredCnchQuery(const IAST * ast)
{
    if (!ast)
        return true;

    if (const auto * create_ast = ast->as<ASTCreateQuery>(); create_ast && create_ast->select)
    {
        return true;
    }
    else if (const auto * ast_update = ast->as<ASTUpdateQuery>(); ast_update && !ast_update->database.empty() && !ast_update->table.empty())
        return isMonitoredCnchTable(ast_update->database);
    else if (const auto * ast_delete = ast->as<ASTDeleteQuery>(); ast_delete && !ast_delete->database.empty() && !ast_delete->table.empty())
        return isMonitoredCnchTable(ast_delete->database);
    else if (const auto * ast_insert = ast->as<ASTInsertQuery>(); ast_insert && !ast_insert->table_id.database_name.empty() && !ast_insert->table_id.database_name.empty())
        return isMonitoredCnchTable(ast_insert->table_id.database_name);
    else if (const auto * ast_select = ast->as<ASTSelectWithUnionQuery>())
    {
        ASTs all_tables;
        bool dummy = false;
        ASTSelectQuery::collectAllTables(ast_select, all_tables, dummy);

        for (const auto & table : all_tables)
        {
            auto database_table = IdentifierSemantic::extractDatabaseAndTable(typeid_cast<ASTTableIdentifier &>(*table));
            if (isMonitoredCnchTable(database_table.first))
                return true;
        }
        return false;
    }
    else
        return false;
}

ProcessList::EntryPtr ProcessList::insert(const String & query_, const IAST * ast, ContextPtr query_context, bool force)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    if (client_info.current_query_id.empty())
        throw Exception("Query id cannot be empty", ErrorCodes::LOGICAL_ERROR);

    auto query_type = getProcessListQueryType(ast);
    auto sub_query_type = ProcessListSubQueryType::Simple;
    bool is_unlimited_query = isUnlimitedQuery(ast);
    bool is_vw_unlimited {false};

    IResourceGroup::Container::iterator group_it;
    IResourceGroup * resource_group = nullptr;

    String type = Poco::toLower(String(ProcessListHelper::toString(query_type)));
    if (query_type == ProcessListQueryType::Default && query_context->getSettingsRef().enable_optimizer)
    {
        sub_query_type = ProcessListSubQueryType::Complex;
    }
    String sub_type = Poco::toLower(String(ProcessListHelper::toString(sub_query_type)));
    LabelledMetrics::MetricLabels labels {{"query_type", type}, {"processing_stage", "processing"}, {"sub_query_type", sub_type}};
    if (!is_unlimited_query && query_type != ProcessListQueryType::System && isMonitoredCnchQuery(ast))
    {
       const_cast<Context *>(query_context.get())->setResourceGroup(ast);
        /// FIXME(xuruiliang): change getResourceGroup to const getResourceGroup
        resource_group = const_cast<Context *>(query_context.get())->tryGetResourceGroup();

        if (auto vw = query_context->tryGetCurrentVW())
            labels.insert({"vw", vw->getName()});
        if (auto wg = query_context->tryGetCurrentWorkerGroup())
            labels.insert({"wg", wg->getID()});
        LabelledMetrics::increment(LabelledMetrics::VwQuery, 1, labels);
    }
    else
    {
        is_vw_unlimited = true;
        LabelledMetrics::increment(LabelledMetrics::UnlimitedQuery, 1, labels);
    }

    if (resource_group != nullptr)
        group_it = resource_group->run(*query_context);

    {
        {
            std::unique_lock lock(mutex);
            auto processes_size = processes.size();
            const auto queue_max_wait_ms = settings.queue_max_wait_ms.totalMilliseconds();
            if (!is_unlimited_query && max_size && processes_size >= max_size)
            {
                if (queue_max_wait_ms)
                    LOG_WARNING(getLogger("ProcessList"), "Too many simultaneous queries, will wait {} ms.", queue_max_wait_ms);
                if (!queue_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(queue_max_wait_ms), [&]{ return processes.size() < max_size; }))
                    throw Exception("Too many simultaneous queries. Maximum: " + toString(max_size), ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
            }
            lock.unlock();
            /**
             * `max_size` check above is controlled by `max_concurrent_queries` server setting and is a "hard" limit for how many
             * queries the server can process concurrently. It is configured at startup. When the server is overloaded with queries and the
             * hard limit is reached it is impossible to connect to the server to run queries for investigation.
             *
             * With `max_concurrent_queries_for_all_users` it is possible to configure an additional, runtime configurable, limit for query concurrency.
             * Usually it should be configured just once for `default_profile` which is inherited by all users. DBAs can override
             * this setting when connecting to ClickHouse, or it can be configured for a DBA profile to have a value greater than that of
             * the default profile (or 0 for unlimited).
             *
             * One example is to set `max_size=X`, `max_concurrent_queries_for_all_users=X-10` for default profile,
             * and `max_concurrent_queries_for_all_users=0` for DBAs or accounts that are vital for ClickHouse operations (like metrics
             * exporters).
             *
             * Another creative example is to configure `max_concurrent_queries_for_all_users=50` for "analyst" profiles running adhoc queries
             * and `max_concurrent_queries_for_all_users=100` for "customer facing" services. This way "analyst" queries will be rejected
             * once is already processing 50+ concurrent queries (including analysts or any other users).
             */

            if (!is_unlimited_query && settings.max_concurrent_queries_for_all_users
                && processes_size >= settings.max_concurrent_queries_for_all_users)
                throw Exception(
                    "Too many simultaneous queries for all users. Current: " + toString(processes_size)
                    + ", maximum: " + settings.max_concurrent_queries_for_all_users.toString(),
                    ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
        }

        checkRunningQuery(query_context, is_unlimited_query, force);

        CurrentMetrics::Metric query_type_metric = getQueryTypeMetric(query_type);
        auto query_status = std::make_shared<QueryStatus>(query_context, query_, client_info, priorities.insert(settings.priority),
            resource_group == nullptr ? nullptr : resource_group->insert(group_it), query_type_metric, is_vw_unlimited);

        auto processes_ret = processes.try_emplace(client_info.current_query_id, query_status);
        if (!processes_ret.second)
        {
            throw Exception("Query with id = " + client_info.current_query_id + " create query status failed.",
                    ErrorCodes::LOGICAL_ERROR);
        }

        res = std::make_shared<Entry>(*this, query_status);

        std::unique_lock lock(mutex);
        query_status->type = query_type;
        ProcessListForUser & user_process_list = user_to_queries[client_info.current_user];

        auto query_it = user_process_list.queries.emplace(client_info.current_query_id, query_status);
        if (!query_it.second)
        {
            LOG_ERROR(getLogger("ProcessList"), "Logical error: cannot insert Querystatus into user_process_list");
        }
        lock.unlock();

        query_status->setUserProcessList(&user_process_list);

        /// Track memory usage for all simultaneously running queries from single user.
        user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage_for_user);
        user_process_list.user_memory_tracker.setDescription("(for user)");

        /// Actualize thread group info
        if (auto thread_group = CurrentThread::getGroup())
        {
            std::lock_guard lock_thread_group(thread_group->mutex);
            thread_group->performance_counters.setParent(&user_process_list.user_performance_counters);
            thread_group->memory_tracker.setParent(&user_process_list.user_memory_tracker);
            thread_group->query = query_status->query;
            thread_group->normalized_query_hash = normalizedQueryHash<false>(query_status->query);

            /// Set query-level memory trackers
            thread_group->memory_tracker.setOrRaiseHardLimit(settings.max_memory_usage);

            if (query_context->hasTraceCollector())
            {
                /// Set up memory profiling
                thread_group->memory_tracker.setOrRaiseProfilerLimit(settings.memory_profiler_step);
                thread_group->memory_tracker.setProfilerStep(settings.memory_profiler_step);
                thread_group->memory_tracker.setSampleProbability(settings.memory_profiler_sample_probability);
            }

            thread_group->memory_tracker.setDescription("(for query)");
            if (settings.memory_tracker_fault_probability)
                thread_group->memory_tracker.setFaultProbability(settings.memory_tracker_fault_probability);

            /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
            ///  since allocation and deallocation could happen in different threads

            query_status->thread_group = std::move(thread_group);
        }

        if (!user_process_list.user_throttler)
        {
            if (settings.max_network_bandwidth_for_user)
                user_process_list.user_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_user, total_network_throttler);
            else if (settings.max_network_bandwidth_for_all_users)
                user_process_list.user_throttler = total_network_throttler;
        }

        if (!total_network_throttler && settings.max_network_bandwidth_for_all_users)
        {
            total_network_throttler = std::make_shared<Throttler>(settings.max_network_bandwidth_for_all_users);
        }
    }

    if (resource_group != nullptr)
        (*group_it)->query_status = &res->get();

    return res;
}

void ProcessList::checkRunningQuery(ContextPtr query_context, bool is_unlimited_query, bool force)
{
    /** Why we use current user?
      * Because initial one is passed by client and credentials for it is not verified,
      *  and using initial_user for limits will be insecure.
      *
      * Why we use current_query_id?
      * Because we want to allow distributed queries that will run multiple secondary queries on same server,
      *  like SELECT count() FROM remote('127.0.0.{1,2}', system.numbers)
      *  so they must have different query_ids.
      */

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();
    std::unique_lock lock(mutex);
    auto user_process_list = user_to_queries.find(client_info.current_user);

    if (user_process_list != user_to_queries.end())
    {
        if (!is_unlimited_query && settings.max_concurrent_queries_for_user
            && user_process_list->second.queries.size() >= settings.max_concurrent_queries_for_user)
            throw Exception("Too many simultaneous queries for user " + client_info.current_user
                + ". Current: " + toString(user_process_list->second.queries.size())
                + ", maximum: " + settings.max_concurrent_queries_for_user.toString(),
                ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);

        auto running_query = user_process_list->second.queries.find(client_info.current_query_id);

        if (running_query != user_process_list->second.queries.end())
        {
            if (!force && !settings.replace_running_query)
                throw Exception("Query with id = " + client_info.current_query_id + " is already running.",
                    ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

            /// Ask queries to cancel. They will check this flag.
            running_query->second->is_killed.store(QueryStatus::EXTERNAL_KILL_BIT, std::memory_order_relaxed);

            const auto replace_running_query_max_wait_ms = settings.replace_running_query_max_wait_ms.totalMilliseconds();
            if (!replace_running_query_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms),
                [&]
                {
                    running_query = user_process_list->second.queries.find(client_info.current_query_id);
                    if (running_query == user_process_list->second.queries.end())
                        return true;
                    running_query->second->is_killed.store(QueryStatus::EXTERNAL_KILL_BIT, std::memory_order_relaxed);
                    return false;
                }))
            {
                throw Exception("Query with id = " + client_info.current_query_id + " is already running and can't be stopped",
                    ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
            }
         }
    }

    /// Check other users running query with our query_id
    for (const auto & process_list_for_user : user_to_queries)
    {
        if (process_list_for_user.first == client_info.current_user)
            continue;
        if (auto running_query = process_list_for_user.second.queries.find(client_info.current_query_id); running_query != process_list_for_user.second.queries.end())
            throw Exception("Query with id = " + client_info.current_query_id + " is already running by user " + process_list_for_user.first,
                ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
    }
}

ProcessListEntry::~ProcessListEntry()
{
    /// Destroy all streams to avoid long lock of ProcessList
    it->releaseQueryStreams();

    String user = it->getClientInfo().current_user;
    String query_id = it->getClientInfo().current_query_id;

    /// This removes the memory_tracker of one request.
    parent.processes.erase(query_id);

    /// Reset throttler, similarly (see above).
    if (parent.processes.empty())
        parent.total_network_throttler.reset();

    std::unique_lock lock(parent.mutex);

    /// Wait for the query if it is in the cancellation right now.
    parent.cancelled_cv.wait(lock, [&]() { return it->is_cancelling == false; });

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(getLogger("ProcessList"), "Logical error: cannot find user in ProcessList");
    }

    ProcessListForUser & user_process_list = user_process_list_it->second;
    if (!user_process_list.queries.erase(query_id))
    {
        LOG_ERROR(getLogger("ProcessList"), "Logical error: cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
    }

    parent.have_space.notify_all();

    /// If there are no more queries for the user, then we will reset memory tracker and network throttler.
    if (user_process_list.queries.empty())
        user_process_list.resetTrackers();
}


QueryStatus::QueryStatus(
    ContextPtr context_,
    const String & query_,
    const ClientInfo & client_info_,
    QueryPriorities::Handle && priority_handle_,
    IResourceGroup::Handle && resource_group_handle_,
    CurrentMetrics::Metric & query_type_metric_,
    const bool is_unlimited_)
    : WithContext(context_)
    , query(query_)
    , client_info(client_info_)
    , priority_handle(std::move(priority_handle_))
    , resource_group_handle(std::move(resource_group_handle_))
    , num_queries_increment{CurrentMetrics::Query}
    , query_type_increment{query_type_metric_}
    , is_unlimited(is_unlimited_)
{
    auto settings = getContext()->getSettings();
    limits.max_execution_time = settings.max_execution_time;
    overflow_mode = settings.timeout_overflow_mode;
    graphviz = std::make_shared<std::vector<std::pair<String, String>>>();
    thread_group.reset();
}

QueryStatus::~QueryStatus()
{
    assert(executors.empty());
}

void QueryStatus::setQueryStreams(const BlockIO & io)
{
    std::lock_guard lock(query_streams_mutex);

    query_stream_in = io.in;
    query_stream_out = io.out;
    query_streams_status = QueryStreamsStatus::Initialized;
}

void QueryStatus::setQueryRewriteByView(const String & rewrite_query)
{
    query_rewrite_by_view = rewrite_query;
}

void QueryStatus::releaseQueryStreams()
{
    BlockInputStreamPtr in;
    BlockOutputStreamPtr out;

    {
        std::lock_guard lock(query_streams_mutex);

        query_streams_status = QueryStreamsStatus::Released;
        in = std::move(query_stream_in);
        out = std::move(query_stream_out);
    }

    /// Destroy streams outside the mutex lock
}

bool QueryStatus::streamsAreReleased()
{
    std::lock_guard lock(query_streams_mutex);

    return query_streams_status == QueryStreamsStatus::Released;
}

bool QueryStatus::tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const
{
    std::lock_guard lock(query_streams_mutex);

    if (query_streams_status != QueryStreamsStatus::Initialized)
        return false;

    in = query_stream_in;
    out = query_stream_out;
    return true;
}

CancellationCode QueryStatus::cancelQuery(bool kill, bool internal)
{
    UInt8 kill_flag = internal ? INTERNAL_KILL_BIT : EXTERNAL_KILL_BIT;
    {
        std::lock_guard lock(executors_mutex);
        if (!executors.empty())
        {
            if (is_killed.load())
                return CancellationCode::CancelSent;

            is_killed.store(kill_flag);

            for (auto * e : executors)
                e->cancel();

            return CancellationCode::CancelSent;
        }
    }

    /// Streams are destroyed, and ProcessListElement will be deleted from ProcessList soon. We need wait a little bit
    if (streamsAreReleased())
        return CancellationCode::CancelSent;

    BlockInputStreamPtr input_stream;
    BlockOutputStreamPtr output_stream;

    if (tryGetQueryStreams(input_stream, output_stream))
    {
        if (input_stream)
        {
            input_stream->cancel(kill);
            return CancellationCode::CancelSent;
        }
        return CancellationCode::CancelCannotBeSent;
    }
    /// Query is not even started
    is_killed.store(kill_flag);
    return CancellationCode::CancelSent;
}

void QueryStatus::addPipelineExecutor(PipelineExecutor * e)
{
    std::lock_guard lock(executors_mutex);
    assert(std::find(executors.begin(), executors.end(), e) == executors.end());
    executors.push_back(e);
}

void QueryStatus::removePipelineExecutor(PipelineExecutor * e)
{
    std::lock_guard lock(executors_mutex);
    assert(std::find(executors.begin(), executors.end(), e) != executors.end());
    std::erase_if(executors, [e](PipelineExecutor * x) { return x == e; });
}

void QueryStatus::dumpPipelineInfo(PipelineExecutor * e)
{
    //Also need lock at QueryStatus::getInfo, but it causes performance down
    //std::lock_guard lock(executors_mutex);
    pipeline_info += e->dumpPipeline();
}

bool QueryStatus::checkCpuTimeLimit(String node_name)
{
    if (is_killed.load())
        return false;

    ContextPtr context = thread_group->query_context.lock();
    const Settings & settings = context->getSettingsRef();
    // query thread group Counters.
    if (settings.max_query_cpu_seconds > 0 && thread_group != nullptr)
    {
        UInt64 total_query_cpu_micros = thread_group->performance_counters[ProfileEvents::SystemTimeMicroseconds]
                + thread_group->performance_counters[ProfileEvents::UserTimeMicroseconds];
        UInt64 thread_cpu_micros = CurrentThread::getProfileEvents()[ProfileEvents::SystemTimeMicroseconds]
                + CurrentThread::getProfileEvents()[ProfileEvents::UserTimeMicroseconds];

        double total_query_cpu_seconds = total_query_cpu_micros * 1.0 / 1000000;
        double thread_cpu_seconds = thread_cpu_micros * 1.0 / 1000000;

        LOG_TRACE(getLogger("ThreadStatus"), "node {} checkCpuTimeLimit thread cpu secs = {}, total cpu secs = {}, max = {}",
                    node_name, thread_cpu_seconds, total_query_cpu_seconds, settings.max_query_cpu_seconds);
        if (total_query_cpu_micros > settings.max_query_cpu_seconds * 1000000)
        {
            switch (overflow_mode)
            {
                case OverflowMode::THROW:
                    throw Exception("Query cpu exceeded: elapsed " + toString(total_query_cpu_seconds)
                                  + " seconds, maximum: " + toString(static_cast<double>(settings.max_query_cpu_seconds)), ErrorCodes::QUERY_CPU_TIMEOUT_EXCEEDED);
                case OverflowMode::BREAK:
                    return true;
                default:
                    throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    return true;
}

void QueryStatus::throwKilledException()
{
    throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
}

bool QueryStatus::checkTimeLimit()
{
    if (auto kill_flag = is_killed.load(); kill_flag)
    {
        throw Exception(
            "Query was cancelled", kill_flag & INTERNAL_KILL_BIT ? ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL : ErrorCodes::QUERY_WAS_CANCELLED);
    }

    return limits.checkTimeLimit(watch, overflow_mode);
}

bool QueryStatus::checkTimeLimitSoft()
{
    if (is_killed.load())
        return false;

    return limits.checkTimeLimit(watch, OverflowMode::BREAK);
}


void QueryStatus::setUserProcessList(ProcessListForUser * user_process_list_)
{
    user_process_list = user_process_list_;
}


ThrottlerPtr QueryStatus::getUserNetworkThrottler()
{
    if (!user_process_list)
        return {};
    return user_process_list->user_throttler;
}

std::shared_ptr<QueryStatus> ProcessList::tryGetProcessListElement(const String & current_query_id, const String & current_user)
{
    auto user_it = user_to_queries.find(current_user);
    if (user_it != user_to_queries.end())
    {
        const auto & user_queries = user_it->second.queries;
        auto query_it = user_queries.find(current_query_id);

        if (query_it != user_queries.end())
            return query_it->second;
    }

    return nullptr;
}


CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill)
{
    std::shared_ptr<QueryStatus> elem;

    /// Cancelling the query should be done without the lock.
    ///
    /// Since it may be not that trivial, for example in case of distributed
    /// queries it tries to cancel the query gracefully on shards and this can
    /// take a while, so acquiring a lock during this time will lead to wait
    /// all new queries for this cancellation.
    ///
    /// Another problem is that it can lead to a deadlock, because of
    /// OvercommitTracker.
    ///
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        std::lock_guard lock(mutex);
        elem = tryGetProcessListElement(current_query_id, current_user);
        if (!elem)
            return CancellationCode::NotFound;
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        std::lock_guard lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(kill, false);
}


void ProcessList::killAllQueries()
{
    std::vector<std::shared_ptr<QueryStatus>> cancelled_processes;

    SCOPE_EXIT({
        std::lock_guard lock(mutex);
        for (auto & cancelled_process : cancelled_processes)
            cancelled_process->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    {
        std::lock_guard lock(mutex);
        cancelled_processes.reserve(processes.size());
        for (const auto & [user, user_queries] : user_to_queries)
        {
            for (const auto & [query_id, status] : user_queries.queries)
            {
                cancelled_processes.push_back(status);
                status->is_cancelling = true;
            }
        }
    }

    for (auto & cancelled_process : cancelled_processes)
    {
        try
        {
            cancelled_process->cancelQuery(true, false);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Kill query failed with id: "+ cancelled_process->client_info.current_query_id + "");
        }
    }
}


QueryStatusInfo QueryStatus::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    QueryStatusInfo res{};

    res.query             = query;
    /// TODO @canh: fix me for union query
    res.client_info       = client_info;
    res.query_rewrite_by_view = query_rewrite_by_view;
    res.elapsed_seconds   = watch.elapsedSeconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.read_rows         = progress_in.read_rows;
    res.read_bytes        = progress_in.read_bytes;
    res.total_rows        = progress_in.total_rows_to_read;
    res.disk_cache_read_bytes  = progress_in.disk_cache_read_bytes;

    /// TODO: Use written_rows and written_bytes when real time progress is implemented
    res.written_rows      = progress_out.read_rows;
    res.written_bytes     = progress_out.read_bytes;
    res.written_duration  = progress_out.written_elapsed_milliseconds;

    res.operator_level    = pipeline_info;

    if (thread_group)
    {
        res.memory_usage = thread_group->memory_tracker.get();
        res.peak_memory_usage = thread_group->memory_tracker.getPeak();
        res.max_io_time_thread_name = thread_group->max_io_time_thread_name;
        res.max_io_time_thread_ms = thread_group->max_io_time_thread_ms;

        if (get_thread_list)
        {
            std::lock_guard lock(thread_group->mutex);
            res.thread_ids = thread_group->thread_ids;
        }

        if (get_profile_events)
        {
            res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
            res.max_io_thread_profile_counters = thread_group->max_io_thread_profile_counters;
        }

    }

    auto context_ptr = context.lock();
    // if context is expired before process list, skip following info items
    if (!context_ptr)
    {
        return res;
    }
    if (get_settings)
    {
        res.query_settings = std::make_shared<Settings>(context_ptr->getSettings());
        res.current_database = context_ptr->getCurrentDatabase();
    }

    return res;
}

ProcessList::Info ProcessList::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    Info per_query_infos;

    std::unique_lock lock(mutex);

    for (const auto & [user, user_queries] : user_to_queries)
    {
        for (const auto & [query_id, status] : user_queries.queries)
        {
            per_query_infos.emplace_back(status->getInfo(get_thread_list, get_profile_events, get_settings));
        }
    }

    lock.unlock();

    return per_query_infos;
}


ProcessListForUser::ProcessListForUser() = default;


ProcessListForUserInfo ProcessListForUser::getInfo(bool get_profile_events) const
{
    ProcessListForUserInfo res;

    res.memory_usage = user_memory_tracker.get();
    res.peak_memory_usage = user_memory_tracker.getPeak();

    if (get_profile_events)
        res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(user_performance_counters.getPartiallyAtomicSnapshot());

    return res;
}


ProcessList::UserInfo ProcessList::getUserInfo(bool get_profile_events) const
{
    UserInfo per_user_infos;

    std::lock_guard lock(mutex);

    per_user_infos.reserve(user_to_queries.size());

    for (const auto & [user, user_queries] : user_to_queries)
        per_user_infos.emplace(user, user_queries.getInfo(get_profile_events));

    return per_user_infos;
}

}
