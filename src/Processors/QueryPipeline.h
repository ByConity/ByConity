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

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/IProcessor.h>
#include <Processors/Pipe.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/Cache/QueryCache.h> /// nested classes such as QC::Writer can't be fwd declared

namespace DB
{

class IOutputFormat;

class QueryPipelineProcessorsCollector;

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class QueryPlan;

struct SubqueryForSet;
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

struct SizeLimits;

struct ExpressionActionsSettings;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

using RuntimeFilterId = UInt32;

class QueryPipeline
{
public:
    QueryPipeline() = default;
    ~QueryPipeline() = default;
    QueryPipeline(QueryPipeline &&) = default;
    QueryPipeline(const QueryPipeline &) = delete;
    QueryPipeline & operator= (QueryPipeline && rhs) = default;
    QueryPipeline & operator= (const QueryPipeline & rhs) = delete;

    /// All pipes must have same header.
    void init(Pipe pipe);
    /// Clear and release all resources.
    void reset();

    bool initialized() { return !pipe.empty(); }
    bool isCompleted() { return pipe.isCompleted(); }

    using StreamType = Pipe::StreamType;

    /// Add transform with simple input and simple output for each port.
    void addSimpleTransform(const Pipe::ProcessorGetter & getter);
    void addSimpleTransform(const Pipe::ProcessorGetterWithStreamKind & getter);
    /// Add transform with getNumStreams() input ports.
    void addTransform(ProcessorPtr transform);
    void addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes);

    using Transformer = std::function<Processors(OutputPortRawPtrs ports)>;
    /// Transform pipeline in general way.
    void transform(const Transformer & transforme, size_t sink_num = 0);

    /// Add TotalsHavingTransform. Resize pipeline to single input. Adds totals port.
    void addTotalsHavingTransform(ProcessorPtr transform);
    /// Add transform which calculates extremes. This transform adds extremes port and doesn't change inputs number.
    void addExtremesTransform();
    /// Resize pipeline to single output and add IOutputFormat. Pipeline will be completed after this transformation.
    void setOutputFormat(ProcessorPtr output);
    /// Get current OutputFormat.
    IOutputFormat * getOutputFormat() const { return output_format; }
    /// Sink is a processor with single input port and no output ports. Creates sink for each output port.
    /// Pipeline will be completed after this transformation.
    void setSinks(const Pipe::ProcessorGetterWithStreamKind & getter);

    /// Add totals which returns one chunk with single row with defaults.
    void addDefaultTotals();

    /// set Transform which will transfer data from totals_port to output ports if totals_port is not empty
    void setTotalsPortToMainPortTransform();
    /// set Transform which will transfer data from extremes_port to output ports if totals_port is not empty
    void setExtremesPortToMainPortTransform();

    /// Forget about current totals and extremes. It is needed before aggregation, cause they will be calculated again.
    void dropTotalsAndExtremes();

    /// Will read from this stream after all data was read from other streams.
    void addDelayedStream(ProcessorPtr source);

    void addMergingAggregatedMemoryEfficientTransform(AggregatingTransformParamsPtr params, size_t num_merging_processors);

    /// Changes the number of output ports if needed. Adds ResizeTransform.
    void resize(size_t num_streams, bool force = false, bool strict = false);

    /// Unite several pipelines together. Result pipeline would have common_header structure.
    /// If collector is used, it will collect only newly-added processors, but not processors from pipelines.
    static QueryPipeline unitePipelines(
            std::vector<std::unique_ptr<QueryPipeline>> pipelines,
            size_t max_threads_limit = 0,
            Processors * collected_processors = nullptr);

    /// Join two pipelines together using JoinPtr.
    /// If collector is used, it will collect only newly-added processors, but not processors from pipelines.
    static std::unique_ptr<QueryPipeline> joinPipelines(
        std::unique_ptr<QueryPipeline> left,
        std::unique_ptr<QueryPipeline> right,
        JoinPtr join,
        size_t max_block_size,
        size_t max_streams,
        bool keep_left_read_in_order,
        bool join_parallel_left_right,
        Processors * collected_processors = nullptr,
        bool need_build_runtime_filter = false);

    /// Add other pipeline and execute it before current one.
    /// Pipeline must have empty header, it should not generate any chunk.
    /// This is used for CreatingSets.
    void addPipelineBefore(QueryPipeline pipeline);

    void addCreatingSetsTransform(const Block & res_header, SubqueryForSet subquery_for_set, const SizeLimits & limits, ContextPtr context);

    PipelineExecutorPtr execute();

    size_t getNumStreams() const { return pipe.numOutputPorts(); }

    bool hasTotals() const { return pipe.getTotalsPort() != nullptr; }

    const Block & getHeader() const { return pipe.getHeader(); }
    void addTableLock(TableLockHolder lock) { pipe.addTableLock(std::move(lock)); }
    void addInterpreterContext(std::shared_ptr<const Context> context) { pipe.addInterpreterContext(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { pipe.addStorageHolder(std::move(storage)); }
    void addCacheHolder(CacheHolderPtr cache_holder) { pipe.addCacheHolder(std::move(cache_holder)); }
    void addQueryPlan(std::unique_ptr<QueryPlan> plan) { pipe.addQueryPlan(std::move(plan)); }
    void setLimits(const StreamLocalLimits & limits) { pipe.setLimits(limits); }
    void setLeafLimits(const SizeLimits & limits) { pipe.setLeafLimits(limits); }
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_)
    {
        pipe.setQuota(quota_);
        quota = quota_;
    }

    /// For compatibility with IBlockInputStream.
    void setProgressCallback(const ProgressCallback & callback);
    void setProcessListElement(QueryStatus * elem);
    void setInternalProgressCallback(const ProgressCallback & callback);

    void writeResultIntoQueryCache(std::shared_ptr<QueryCache::Writer> query_cache_writer);
    void readFromQueryCache(
        std::unique_ptr<SourceFromChunks> source,
        std::unique_ptr<SourceFromChunks> source_totals,
        std::unique_ptr<SourceFromChunks> source_extremes);

    void finalizeWriteInQueryCache();

    /// Create progress callback from limits and quotas.
    std::unique_ptr<ReadProgressCallback> getReadProgressCallback() const;
    /// Skip updating profile events.
    /// For merges in mutations it may need special logic, it's done inside ProgressCallback.
    void disableProfileEventUpdate() { update_profile_events = false; }

    /// Recommend number of threads for pipeline execution.
    size_t getNumThreads() const
    {
        auto num_threads = pipe.maxParallelStreams();

        if (max_threads) //-V1051
            num_threads = std::min(num_threads, max_threads);

        return std::max<size_t>(min_threads + 1, num_threads);
    }

    /// Set upper limit for the recommend number of threads
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }

    /// Set lower limit for the number of threads
    void setMinThreads(size_t min_threads_) { min_threads = min_threads_; }

    /// Try increase the min number of threads
    void limitMinThreads(size_t min_threads_) { min_threads = std::max(min_threads_, min_threads); }

    /// Update upper limit for the recommend number of threads
    void limitMaxThreads(size_t max_threads_)
    {
        if (max_threads == 0 || max_threads_ < max_threads)
            max_threads = max_threads_;
    }

    const Processors & getProcessors() const { return pipe.getProcessors(); }

    const CacheHolderPtr getCacheHolder() const;

    void setWriteCacheComplete(const ContextPtr & context);

    void clearUncompletedCache(const ContextPtr & context);

    /// Convert query pipeline to pipe.
    static Pipe getPipe(QueryPipeline pipeline) { return std::move(pipeline.pipe); }

    Processors & getPipeProcessors() { return pipe.processors; }

    void complete(std::shared_ptr<IOutputFormat> format);

    /// use for query cache
    void addUsedStorageIDs(const std::set<StorageID> & storage_id);
    std::set<StorageID> getUsedStorageIDs() const;
    bool hasAllUsedStorageIDs() const;
    void setHasAllUsedStorageIDs(bool val);
private:

    Pipe pipe;
    IOutputFormat * output_format = nullptr;

    ProgressCallback progress_callback;
    std::shared_ptr<const EnabledQuota> quota;
    bool update_profile_events = true;

    /// Limit on the number of threads. Zero means no limit.
    /// Sometimes, more streams are created then the number of threads for more optimal execution.
    size_t max_threads = 0;

    /// Limit on the minimum number of threads.
    size_t min_threads = 0;
    /// use for query cache to detect which tables is used
    std::set<StorageID> used_storage_ids;
    bool has_all_used_storage_ids = true;

    QueryStatus * process_list_element = nullptr;

    void checkInitialized();
    void checkInitializedAndNotCompleted();

    void initRowsBeforeLimit();

    void setCollectedProcessors(Processors * processors);

    friend class QueryPipelineProcessorsCollector;
    friend class CompletedPipelineExecutor;
};

/// This is a small class which collects newly added processors to QueryPipeline.
/// Pipeline must live longer than this class.
class QueryPipelineProcessorsCollector
{
public:
    explicit QueryPipelineProcessorsCollector(QueryPipeline & pipeline_, IQueryPlanStep * step_ = nullptr);
    ~QueryPipelineProcessorsCollector();

    Processors detachProcessors(size_t group = 0);

private:
    QueryPipeline & pipeline;
    IQueryPlanStep * step;
    Processors processors;
};

}
