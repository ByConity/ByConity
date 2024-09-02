/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Statistics/SubqueryHelper.h>
#include <Common/CurrentThread.h>
#include "Core/UUID.h"

namespace DB::Statistics
{

static ContextMutablePtr createQueryContext(ContextPtr context, bool large_sql)
{
    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    auto old_query_id = context->getInitialQueryId();
    auto new_query_id = old_query_id + "__create_stats_internal__" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    query_context->setCurrentQueryId(new_query_id); // generate random query_id
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    SettingsChanges changes;
    auto enable_optimizer = large_sql;
    changes.emplace_back("enable_optimizer", enable_optimizer);
    changes.emplace_back("dialect_type", "CLICKHOUSE");
    changes.emplace_back("database_atomic_wait_for_drop_and_detach_synchronously", true);
    changes.emplace_back("enable_deterministic_sample_by_range", true);
    changes.emplace_back("uniform_sample_by_range", true);
    changes.emplace_back("insert_distributed_sync", true);
    query_context->applySettingsChanges(changes);

    return query_context;
}

struct SubqueryHelper::DataImpl
{
    ContextPtr old_context;
    ContextMutablePtr subquery_context;
    // std::unique_ptr<CurrentThread::QueryScope> query_scope;
    std::unique_ptr<BlockIO> block_io;
    std::unique_ptr<CurrentThread::QueryScope> query_scope;
    std::unique_ptr<PullingAsyncPipelineExecutor> executor;
};

SubqueryHelper::SubqueryHelper(std::unique_ptr<DataImpl> impl_) : impl(std::move(impl_))
{
}

SubqueryHelper SubqueryHelper::create(ContextPtr context, const String & sql, bool large_sql)
{
    if (context->getSettingsRef().statistics_collect_debug_level >= 1)
    {
        LOG_INFO(&Poco::Logger::get("create stats subquery"), "collect stats with sql: " + sql);
    }
    auto impl = std::make_unique<SubqueryHelper::DataImpl>();
    impl->old_context = context;

    if (large_sql)
        CurrentThread::detachQueryIfNotDetached();
    impl->subquery_context = createQueryContext(context, large_sql);

    if (large_sql)
        impl->query_scope = std::make_unique<CurrentThread::QueryScope>(impl->subquery_context);
    // impl->query_scope = std::make_unique<CurrentThread::QueryScope>(impl->subquery_context);
    bool use_internal = !large_sql;
    impl->block_io
        = std::make_unique<BlockIO>(executeQuery(sql, impl->subquery_context, use_internal, QueryProcessingStage::Complete, false));

    impl->executor = std::make_unique<PullingAsyncPipelineExecutor>(impl->block_io->pipeline);
    return SubqueryHelper(std::move(impl));
}

SubqueryHelper::~SubqueryHelper()
{
    if (impl->block_io)
    {
        impl->block_io->onFinish();
    }
    impl->executor.reset();
    impl->block_io.reset();
    impl->query_scope.reset();
    impl->subquery_context.reset();
}

Block SubqueryHelper::getNextBlock()
{
    if (!impl->executor)
    {
        throw Exception("uninitialized SubqueryHelper", ErrorCodes::LOGICAL_ERROR);
    }

    Block block;
    while (!block && impl->executor->pull(block))
    {
    }
    return block;
}

void executeSubQuery(ContextPtr old_context, const String & sql)
{
    ContextMutablePtr subquery_context = createQueryContext(old_context, false);
    // CurrentThread::QueryScope query_scope(subquery_context);

    String res;
    ReadBufferFromString is1(sql);
    WriteBufferFromString os1(res);

    executeQuery(is1, os1, false, subquery_context, {}, {}, true);
}
}
