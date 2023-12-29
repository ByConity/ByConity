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
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
/// Sorts stream of data. See MergeSortingTransform.
class SortingStep : public ITransformingStep
{
public:
    enum class Type
    {
        Full,
        FinishSorting,
        MergingSorted,
    };

    struct Settings
    {
        size_t max_block_size;
        SizeLimits size_limits;
        size_t max_bytes_before_remerge = 0;
        double remerge_lowered_memory_bytes_ratio = 0;
        size_t max_bytes_before_external_sort = 0;
        TemporaryDataOnDiskScopePtr tmp_data = nullptr;
        size_t min_free_disk_space = 0;

        explicit Settings(const Context & context);
        explicit Settings(size_t max_block_size_);
    };

    explicit SortingStep(
        const DataStream & input_stream_, 
        SortDescription description_, 
        UInt64 limit_, 
        bool partial_, 
        SortDescription prefix_description_ = {});

    /// Full Sorting
    explicit SortingStep(
        const DataStream & input_stream_,
        SortDescription description_,
        UInt64 limit_,
        const Settings & settings_);

    String getName() const override { return "Sorting"; }

    Type getType() const override { return Type::Sorting; }
    const SortDescription & getSortDescription() const { return result_description; }
    const SortDescription & getPrefixDescription() const { return prefix_description; }
    void setPrefixDescription(const SortDescription & prefix_description_) { prefix_description = prefix_description_; }
    UInt64 getLimit() const { return limit; }
    bool isPartial() const { return partial; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void toProto(Protos::SortingStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<SortingStep> fromProto(const Protos::SortingStep & proto, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    const SortDescription result_description;
    UInt64 limit;
    bool partial;
    SortDescription prefix_description;

    Settings sort_settings;
};

}
