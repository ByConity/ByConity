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
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Protos/plan_segment_manager.pb.h>
#include <brpc/controller.h>

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

struct PlanSegmentHeader
{
    PlanSegmentInstanceId instance_id;
    size_t plan_segment_buf_size = 0;
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr;
    UInt32 attempt_id = std::numeric_limits<UInt32>::max();
    SourceTaskFilter source_task_filter;
    void toProto(Protos::PlanSegmentHeader & proto) const
    {
        proto.set_plan_segment_id(instance_id.segment_id);
        proto.set_parallel_id(instance_id.parallel_index);
        proto.set_plan_segment_buf_size(plan_segment_buf_size);
        proto.set_attempt_id(attempt_id);
        if (source_task_filter.isValid())
            *proto.mutable_source_task_filter() = source_task_filter.toProto();
    }
};

struct AddressWithWorkerId
{
    AddressInfo address_info;
    WorkerId worker_id;

    inline bool operator==(AddressWithWorkerId const & rhs) const
    {
        return (this->address_info == rhs.address_info && this->worker_id == rhs.worker_id);
    }
    class Hash
    {
    public:
        size_t operator()(const AddressWithWorkerId & key) const
        {
            return AddressInfo::Hash()(key.address_info);
        }
    };
};
using PlanSegmentHeaders = std::vector<PlanSegmentHeader>;
using BatchPlanSegmentHeaders = std::unordered_map<AddressWithWorkerId, PlanSegmentHeaders, AddressWithWorkerId::Hash>;

BlockIO lazyExecutePlanSegmentLocally(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context);

void executePlanSegmentInternal(
    PlanSegmentInstancePtr plan_segment_instance,
    ContextMutablePtr context,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    bool async);

// void executePlanSegmentLocally(const PlanSegment & plan_segment, ContextPtr initial_query_context);

void cleanupExchangeDataForQuery(const AddressInfo & address, UInt64 & query_unique_id);

void prepareQueryCommonBuf(butil::IOBuf & common_buf, const PlanSegment & any_plan_segment, ContextPtr & context);

void executePlanSegmentRemotelyWithPreparedBuf(
    size_t segment_id,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id = WorkerId{});

void executePlanSegmentsRemotely(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    const butil::IOBuf & query_resource_buf,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id = WorkerId{});
}
