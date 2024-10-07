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

#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>

#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/TableScanStep.h>
#include "common/logger_useful.h"

namespace DB
{

namespace
{
    class RuntimeFilterCollector
    {
    public:
        void visit(PlanSegmentTree::Node & plan_segment_node)
        {
            auto * plan_segment = plan_segment_node.getPlanSegment();
            if (!visited.emplace(plan_segment).second)
                return;

            auto collect_runtime_filters = [&](const ConstASTPtr & filter) {
                for (const auto & runtime_filter : RuntimeFilterUtils::extractRuntimeFilters(filter).first)
                {
                    auto id = RuntimeFilterUtils::extractId(runtime_filter);
                    runtime_filter_probes[id].emplace(plan_segment);
                }
            };

            for (auto & node : plan_segment->getQueryPlan().getNodes())
            {
                if (node.step->getType() == IQueryPlanStep::Type::Join)
                {
                    const auto & join_step = static_cast<const JoinStep &>(*node.step);
                    for (const auto & runtime_filter : join_step.getRuntimeFilterBuilders())
                    {
                        runtime_filter_builds.emplace(runtime_filter.second.id, std::make_pair(plan_segment, &node));
                        if (runtime_filter.second.distribution == RuntimeFilterDistribution::DISTRIBUTED)
                            remote_runtime_filter_builds.emplace(runtime_filter.second.id);
                        else
                            local_runtime_filter_builds.emplace(runtime_filter.second.id);
                    }
                }

                if (node.step->getType() == IQueryPlanStep::Type::Filter)
                {
                    const auto & filter_step = dynamic_cast<const FilterStep &>(*node.step);
                    collect_runtime_filters(filter_step.getFilter());
                }
                else if (node.step->getType() == IQueryPlanStep::Type::TableScan)
                {
                    const auto & table_step = dynamic_cast<const TableScanStep &>(*node.step);
                    if (const auto * filter = table_step.getPushdownFilterCast())
                        collect_runtime_filters(filter->getFilter());
                    auto * query = table_step.getQueryInfo().query->as<ASTSelectQuery>();
                    if (auto query_filter = query->getWhere())
                        collect_runtime_filters(query_filter);
                    if (auto query_filter = query->getPrewhere())
                        collect_runtime_filters(query_filter);
                    if (auto partition_filter = table_step.getQueryInfo().partition_filter)
                        collect_runtime_filters(partition_filter);
                }
            }

            for (auto * child : plan_segment_node.children)
                visit(*child);
        }

        std::unordered_map<RuntimeFilterId, std::pair<PlanSegment *, QueryPlan::Node *>> runtime_filter_builds;
        std::unordered_map<RuntimeFilterId, std::unordered_set<PlanSegment *>> runtime_filter_probes;

        std::unordered_set<RuntimeFilterId> remote_runtime_filter_builds;
        std::unordered_set<RuntimeFilterId> local_runtime_filter_builds;

        std::unordered_set<PlanSegment *> visited;
    };
}

size_t RuntimeFilterCollection::add(RuntimeFilterData data, UInt32 parallel_id)
{
    std::lock_guard guard(mutex);
    /// Shouldn't have this shard_num before, ignore the redundant
    if (!rf_data.contains(parallel_id))
    {
        rf_data[parallel_id] = std::move(data);
    }
    else
    {
         LOG_WARNING(
            getLogger("RuntimeFilterCollection"),
            "build rf receive duplicate id:{} will cause rf timeout", parallel_id);
    }

    return rf_data.size();
}


std::unordered_map<RuntimeFilterId, InternalDynamicData> RuntimeFilterCollection::finalize()
{
    std::lock_guard guard(mutex);
    return builder->extractDistributedValues(builder->merge(std::move(rf_data)));
}
RuntimeFilterManager::~RuntimeFilterManager()
{
    need_stop = true;
    if (check_thread)
    {
        check_thread.reset();
    }
}

void RuntimeFilterManager::initRoutineCheck()
{
    static std::once_flag flag;
    std::call_once(flag, [&](){
        check_thread = std::make_unique<std::thread>([this](){
            while (!need_stop)
            {
                std::this_thread::sleep_for(std::chrono::seconds(30)); /// 30s interval
                if (need_stop) break;
                this->routineCheck();
            }
        });
        check_thread->detach();
    });
}

void RuntimeFilterManager::routineCheck()
{
    if (need_stop || complete_runtime_filters.size() == 0)
        return ;

    LOG_TRACE(log, "start routine check, total rf:{}", complete_runtime_filters.size());
    std::vector<String> expire_keys;
    UInt64 current_time = clock_gettime_ns() / 1000000; /// ms
    auto get_expire_keys = [&](const String & key, const DynamicValuePtr & val_ptr)
    {
        if ((current_time - val_ptr->lastTime()) >= clean_rf_time_limit) /// timeout
        {
            LOG_DEBUG(log, "get expired key:{}, ref:{}, time:{}", key, val_ptr->ref(), val_ptr->lastTime());
            expire_keys.emplace_back(key);
        }
    };

    complete_runtime_filters.computeExpireKeys(std::move(get_expire_keys));
    if (!expire_keys.empty())
    {
        for (const auto & key : expire_keys)
        {
            LOG_DEBUG(log, "do rm expired key:{}", key);
            complete_runtime_filters.remove(key);
        }
    }
}

RuntimeFilterManager & RuntimeFilterManager::getInstance()
{
    static RuntimeFilterManager ret;
    return ret;
}

void RuntimeFilterManager::registerQuery(const String & query_id, PlanSegmentTree & plan_segment_tree, ContextPtr context)
{
    RuntimeFilterCollector collector;
    collector.visit(*plan_segment_tree.getRoot());

    if (collector.runtime_filter_builds.empty())
        return;

    // register remote runtime filters in probe plan segment for resource release
    for (const auto & [id, plan_segments] : collector.runtime_filter_probes) {
        for (const auto & plan_segment : plan_segments)
            plan_segment->addRuntimeFilter(id);
    }

    // register local runtime filters in build plan segment for resource release
    for (const auto & id : collector.local_runtime_filter_builds) {
        const auto & runtime_filter_build = collector.runtime_filter_builds[id];
        runtime_filter_build.first->addRuntimeFilter(id);
    }

    std::unordered_map<RuntimeFilterBuilderId, RuntimeFilterCollectionPtr> builders;
    std::unordered_map<RuntimeFilterId, std::unordered_set<size_t>> targets;

    // register remote runtime filters information in coordinator
    for (const auto & id : collector.remote_runtime_filter_builds) {
        const auto & runtime_filter_build = collector.runtime_filter_builds[id];
        const auto * plan_segment = runtime_filter_build.first;
        auto * join_step = dynamic_cast<JoinStep *>(runtime_filter_build.second->step.get());
        auto builder = join_step->createRuntimeFilterBuilder(context);
        builders.emplace(builder->getId(), std::make_shared<RuntimeFilterCollection>(builder, plan_segment->getParallelSize()));
        for (const auto * probe_plan_segment : collector.runtime_filter_probes[id])
            targets[id].emplace(probe_plan_segment->getPlanSegmentId());
    }

    if (log->debug())
    {
        std::stringstream ss;
        ss << "register runtime filter:\n";
        ss << "  locale runtime filter: ";
        for (const auto & id : collector.local_runtime_filter_builds)
            ss << id << ", ";
        ss << "\n";
        ss << "  remote runtime filter:\n";
        for (const auto & item : targets)
        {
            ss << "    id:" << item.first << " , target segment id: ";
            for (const auto & id : item.second)
            {
                ss << id << ", ";
            }
            ss << "\n";
        }
        LOG_DEBUG(log, ss.str());
    }

    auto runtime_filter_collection_context = std::make_shared<RuntimeFilterCollectionContext>(std::move(builders), std::move(targets));
    runtime_filter_collection_contexts.put(query_id, runtime_filter_collection_context);
    clean_rf_time_limit = context->getSettingsRef().clean_rf_time_limit;
}

void RuntimeFilterManager::removeQuery(const String & query_id)
{
    runtime_filter_collection_contexts.remove(query_id);
}

void RuntimeFilterManager::addDynamicValue(
    const String & query_id, RuntimeFilterId filter_id, DynamicData && dynamic_value, UInt32 ref_segment)
{
    LOG_TRACE(getLogger("RuntimeFilterManager"), "addDynamicValue: {}, {}", filter_id, dynamic_value.dump());
    complete_runtime_filters
        .compute(
            makeKey(query_id, filter_id),
            [](const auto &, DynamicValuePtr value) {
                if (!value)
                    return std::make_shared<DynamicValue>();
                return value;
            })
        ->set(std::move(dynamic_value), ref_segment);
}

DynamicValuePtr RuntimeFilterManager::getDynamicValue(const String & query_id, RuntimeFilterId filter_id)
{
    return getDynamicValue(makeKey(query_id, filter_id));
}

DynamicValuePtr RuntimeFilterManager::getDynamicValue(const String & key)
{
    return complete_runtime_filters.compute(key, [](const auto &, DynamicValuePtr value) {
        if (!value)
            return std::make_shared<DynamicValue>();
        return value;
    });
}

void RuntimeFilterManager::removeDynamicValue(const String & query_id, RuntimeFilterId filter_id)
{
    auto key = makeKey(query_id, filter_id);
    auto ref = getDynamicValue(makeKey(query_id, filter_id))->unref();
    if (ref <= 0) {
        complete_runtime_filters.remove(key);
    }
}

String RuntimeFilterManager::makeKey(const String & query_id, RuntimeFilterId filter_id)
{
    return query_id + "_" + std::to_string(filter_id);
}

DynamicData & DynamicValue::get(size_t timeout_ms)
{
    if (timeout_ms == 0 || ready)
    {
        return value;
    }
    else
    {
        auto wait_duration = std::chrono::milliseconds(1);
        int left = timeout_ms;
        while (!ready && left > 0)
        {
            std::this_thread::sleep_for(wait_duration);
            left-- ;
        }

        return value;
    }
}

void DynamicValue::set(DynamicData&& value_, UInt32 ref)
{
    value = std::move(value_);
    ref_count = ref;
    last_time = clock_gettime_ns() / 1000000; /// ms
    ready = true;
}

bool DynamicValue::isReady()
{
    return ready;
}

String DynamicValue::dump()
{
    if (!isReady())
        return "(not ready)";
    else
    {
        auto const & data = get(0);
        if (data.is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(data.data);
            return d.dump();
        }
        else
        {
            auto const & d = std::get<InternalDynamicData>(data.data);
            return "bf:" + d.bf.dump() + " set:" + d.set.dump() + " range:" + d.range.dump();
        }
    }
}

}
