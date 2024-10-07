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

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/RuntimeFilter/ConcurrentHashMap.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <bthread/condition_variable.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
using RuntimeFilterId = UInt32;
using RuntimeFilterBuilderId = UInt32;

class RuntimeFilterCollection;
class RuntimeFilterCollectionContext;
class DynamicValue;
struct DynamicData;
using RuntimeFilterCollectionPtr = std::shared_ptr<RuntimeFilterCollection>;
using RuntimeFilterCollectionContextPtr = std::shared_ptr<RuntimeFilterCollectionContext>;
using DynamicValuePtr = std::shared_ptr<DynamicValue>;

class RuntimeFilterCollection
{
public:
    RuntimeFilterCollection(RuntimeFilterBuilderPtr builder_, size_t parallel_size_)
        : builder(std::move(builder_)), parallel_size(parallel_size_)
    {
    }

    size_t add(RuntimeFilterData data, UInt32 parallel_id);

    size_t getParallelSize() const { return parallel_size; }

    std::unordered_map<RuntimeFilterId, InternalDynamicData> finalize();

private:
    bthread::Mutex mutex;
    std::map<UInt32, RuntimeFilterData> rf_data;

    RuntimeFilterBuilderPtr builder;
    const size_t parallel_size;
};


/// thread-safe
class RuntimeFilterCollectionContext
{
public:
    RuntimeFilterCollectionContext(
        std::unordered_map<RuntimeFilterBuilderId, RuntimeFilterCollectionPtr> collections_,
        std::unordered_map<RuntimeFilterId, std::unordered_set<size_t>> execute_segment_ids_)
        : collections(collections_), execute_segment_ids(std::move(execute_segment_ids_))
    {
    }

    RuntimeFilterCollectionPtr getCollection(size_t build_id) const { return collections.at(build_id); }

    const std::unordered_set<size_t> & getExecuteSegmentIds(RuntimeFilterId id) const
    {
        auto it = execute_segment_ids.find(id);
        if (it == execute_segment_ids.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "id not found: {}", id);
        return it->second;
    }

private:
    const std::unordered_map<RuntimeFilterBuilderId, RuntimeFilterCollectionPtr> collections;
    const std::unordered_map<RuntimeFilterId, std::unordered_set<size_t>> execute_segment_ids;
};

class DynamicValue
{
public:
    DynamicValue() = default;
    DynamicData & get(size_t timeout_ms);
    void set(DynamicData&& value, UInt32 ref);
    bool isReady();
    UInt32 unref() { return --ref_count; }
    UInt64 lastTime() const { return last_time; }
    UInt32 ref() { return ref_count; }
    String dump();

private:
    DynamicData value;
    std::atomic<bool> ready{false};
    std::atomic<UInt32> ref_count;
    UInt64 last_time;  /// ms
};

class RuntimeFilterManager final : private boost::noncopyable
{
public:
    static RuntimeFilterManager & getInstance();

    /// call before plan segment schedule in coordinator
    void registerQuery(const String & query_id, PlanSegmentTree & plan_segment_tree, ContextPtr context);

    /// call on query finish in coordinator
    void removeQuery(const String & query_id);

    /// merge partial runtime filters received from worker builders
    RuntimeFilterCollectionContextPtr getRuntimeFilterCollectionContext(const String & query_id)
    {
        return runtime_filter_collection_contexts.get(query_id);
    }

    /// call in worker, store runtime filter dispatched from server dispatch
    void addDynamicValue(const String & query_id, RuntimeFilterId filter_id, DynamicData&& dynamic_value, UInt32 ref_segment);

    /// call in worker, clear after plan segment finish
    void removeDynamicValue(const String & query_id, RuntimeFilterId filter_id);

    /// call in worker, get dynamic value for runtime filter
    DynamicValuePtr getDynamicValue(const String & query_id, RuntimeFilterId filter_id);

    /// call in worker, get runtime filter for execution
    DynamicValuePtr getDynamicValue(const String & key);

    static String makeKey(const String & query_id, RuntimeFilterId filter_id);

    void initRoutineCheck();
    ~RuntimeFilterManager();

private:
    void routineCheck();
    RuntimeFilterManager() : log(getLogger("RuntimeFilterManager")) { initRoutineCheck(); }

    /**
     * Coordinator: Query Id -> RuntimeFilters
     */
    ConcurrentHashMap<String, RuntimeFilterCollectionContextPtr> runtime_filter_collection_contexts;

    /**
     * Worker: Query Id, PlanSegment Id, Filter Id -> RuntimeFilters
     */
    ConcurrentHashMap<String, DynamicValuePtr> complete_runtime_filters;

    LoggerPtr log;
    std::unique_ptr<std::thread> check_thread{nullptr};
    std::atomic_bool need_stop = false;
    UInt64 clean_rf_time_limit = 300000; /// default 300s to timeout runtime filters
};
}
