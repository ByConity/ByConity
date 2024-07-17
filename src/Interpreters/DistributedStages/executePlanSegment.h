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

#pragma once

#include <memory>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Protos/plan_segment_manager.pb.h>
#include <brpc/controller.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>

namespace butil
{
class IObuf;
}

namespace std
{
template <>
struct hash<brpc::CallId>
{
    std::size_t operator()(const brpc::CallId & id) const { return hash<uint64_t>{}(id.value); }
};
}

namespace DB
{
class Context;

struct AsyncContext
{
    enum AsyncStats : uint8_t
    {
        INIT = 0,
        SUCCESS = 1,
        FAILED = 2
    };

    struct AsyncResult
    {
        bool is_success{true};
        std::string error_text;
        std::string failed_worker;
        AsyncStats status{AsyncStats::INIT};
        int error_code{0};
    };

    void asyncComplete(brpc::CallId id, AsyncResult & async_result)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (result.status == AsyncStats::FAILED)
            return;
        if (!async_result.is_success)
        {
            result.status = AsyncStats::FAILED;
            result.error_text = std::move(async_result.error_text);
            result.failed_worker = std::move(async_result.failed_worker);
            result.error_code = async_result.error_code;
            lock.unlock();
            cv.notify_all();
            return;
        }
        auto it = call_ids.find(id);
        if (it != call_ids.end())
        {
            if (call_ids.size() == 1)
            {
                //last callid, weak up main  thread
                result.status = AsyncContext::SUCCESS;
                call_ids.erase(it);
                lock.unlock();
                cv.notify_all();
                return;
            }
            else
                call_ids.erase(it);
        }
    }

    void addCallId(brpc::CallId id)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        call_ids.emplace(id);
    }

    AsyncResult wait()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (call_ids.size() == 0)
            return result;
        cv.wait(
            lock, [&] { return (result.status == AsyncStats::SUCCESS && call_ids.size() == 0) || result.status == AsyncStats::FAILED; });
        return result;
    }

    std::unordered_set<brpc::CallId> call_ids;
    bthread::Mutex mutex;
    AsyncResult result;
    bthread::ConditionVariable cv;
};

using AsyncContextPtr = std::shared_ptr<AsyncContext>;

BlockIO lazyExecutePlanSegmentLocally(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context);

void executePlanSegmentInternal(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context, bool async);

// void executePlanSegmentLocally(const PlanSegment & plan_segment, ContextPtr initial_query_context);

void cleanupExchangeDataForQuery(const AddressInfo & address, UInt64 & query_unique_id);

void prepareQueryCommonBuf(butil::IOBuf & common_buf, const PlanSegment & any_plan_segment, ContextPtr & context);

void executePlanSegmentRemotelyWithPreparedBuf(
    const PlanSegment & plan_segment,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id = WorkerId{});
}
