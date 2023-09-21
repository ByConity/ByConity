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

#include <QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

SettingQuotaAndLimitsStep::SettingQuotaAndLimitsStep(
    const DataStream & input_stream_,
    StoragePtr storage_,
    TableLockHolder table_lock_,
    StreamLocalLimits & limits_,
    SizeLimits & leaf_limits_,
    std::shared_ptr<const EnabledQuota> quota_,
    ContextPtr context_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , context(std::move(context_))
    , storage(std::move(storage_))
    , table_lock(std::move(table_lock_))
    , limits(limits_)
    , leaf_limits(leaf_limits_)
    , quota(std::move(quota_))
{
}

void SettingQuotaAndLimitsStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void SettingQuotaAndLimitsStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    /// Table lock is stored inside pipeline here.
    pipeline.setLimits(limits);

    /**
      * Leaf size limits should be applied only for local processing of distributed queries.
      * Such limits allow to control the read stage on leaf nodes and exclude the merging stage.
      * Consider the case when distributed query needs to read from multiple shards. Then leaf
      * limits will be applied on the shards only (including the root node) but will be ignored
      * on the results merging stage.
      */
    if (storage && !storage->isRemote())
        pipeline.setLeafLimits(leaf_limits);

    if (quota)
        pipeline.setQuota(quota);

    /// Order of resources below is important.
    if (context)
        pipeline.addInterpreterContext(std::move(context));

    if (storage)
        pipeline.addStorageHolder(std::move(storage));

    if (table_lock)
        pipeline.addTableLock(std::move(table_lock));
}

std::shared_ptr<IQueryPlanStep> SettingQuotaAndLimitsStep::copy(ContextPtr) const
{
    throw Exception("SettingQuotaAndLimitsStep can not copy", ErrorCodes::NOT_IMPLEMENTED);
}

}
