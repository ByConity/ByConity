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

#include <Access/AccessControlManager.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Statistics/SubqueryHelper.h>
#include "Common/ThreadStatus.h"
#include <Common/CurrentThread.h>
#include "Core/SettingsEnums.h"
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
    changes.emplace_back("ensure_one_mark_in_part_when_sample_by_range", false);
    changes.emplace_back("insert_distributed_sync", true);
    changes.emplace_back("aggregate_functions_null_for_empty", false);
    changes.emplace_back("enable_final_sample", false);
    changes.emplace_back("statistics_return_row_count_if_empty", false);
    changes.emplace_back("limit", 0);
    changes.emplace_back("offset", 0);
    changes.emplace_back("force_primary_key", 0);
    changes.emplace_back("force_index_by_date", 0);
    changes.emplace_back("optimize_skip_unused_shards", 1);
    changes.emplace_back("join_use_nulls", 1);

    // manual xml config should have higher priority, so apply code changes first
    query_context->applySettingsChanges(changes);

    // NOTE: <statistics_internal_sql> can only control the behaviour of internal sqls
    // normal statistics flags are controled by default/user profile
    // TODO(gouguilin): fix me
#if 0
    const String profile_name = "statistics_internal_sql";
    if (query_context->hasProfile(profile_name))
    {
        query_context->setCurrentProfile(profile_name);
    }
#endif

    return query_context;
}

struct SubqueryHelper::DataImpl
{
    ContextPtr old_context;
    ContextMutablePtr subquery_context;
    // std::unique_ptr<CurrentThread::QueryScope> query_scope;
    std::unique_ptr<BlockIO> block_io;
    std::unique_ptr<PullingAsyncPipelineExecutor> executor;
    String old_query_id;
    bool large_sql;

    ~DataImpl()
    {
        if (this->block_io)
        {
            this->block_io->onFinish();
        }
        this->executor.reset();
        this->block_io.reset();

        if (this->large_sql)
        {
            auto & thread_status = CurrentThread::get();
            thread_status.attachQueryContext(this->old_context);

            // to make ThreadStatus destructor happy, we have to reset query_id to the old one
            // before we destruct subquery_context
            this->subquery_context->setCurrentQueryId(this->old_query_id);
        }
        this->subquery_context.reset();
    }
};

SubqueryHelper::SubqueryHelper(std::unique_ptr<DataImpl> impl_) : impl(std::move(impl_))
{
}

SubqueryHelper SubqueryHelper::create(ContextPtr context, const String & sql, bool large_sql)
{
    LOG_TRACE(&Poco::Logger::get("create stats subquery"), "collect stats with sql: " + sql);
    auto impl = std::make_unique<SubqueryHelper::DataImpl>();
    impl->large_sql = large_sql;
    impl->old_context = context;
    impl->old_query_id = context->getCurrentQueryId();
    impl->subquery_context = createQueryContext(context, large_sql);

    bool use_internal = !large_sql;
    if (large_sql)
    {
        // we need to replace CurrentThead context to the using one
        // when executeQuery(internal=0)
        auto & thread_status = CurrentThread::get();
        thread_status.attachQueryContext(impl->subquery_context);
    }

    impl->block_io
        = std::make_unique<BlockIO>(executeQuery(sql, impl->subquery_context, use_internal, QueryProcessingStage::Complete, false));

    impl->executor = std::make_unique<PullingAsyncPipelineExecutor>(impl->block_io->pipeline);
    return SubqueryHelper(std::move(impl));
}

SubqueryHelper::~SubqueryHelper()
{
}

Block SubqueryHelper::getNextBlock()
{
    if (!impl->executor)
    {
        throw Exception("uninitialized SubqueryHelper", ErrorCodes::LOGICAL_ERROR);
    }

    Block block;
    while (!block && impl->executor->pull(block, 0))
    {
    }
    return block;
}

void executeSubQuery(ContextPtr old_context, const String & sql)
{
    ContextMutablePtr subquery_context = createQueryContext(old_context, false);

    String res;
    ReadBufferFromString is1(sql);
    WriteBufferFromString os1(res);

    executeQuery(is1, os1, false, subquery_context, {}, {}, true);
}
}
