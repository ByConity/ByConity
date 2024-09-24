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

#include <Processors/QueryPipeline.h>

#include <IO/WriteHelpers.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IJoin.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/IntermediateResult/CacheManager.h>
#include <Processors/LimitTransform.h>
#include <Processors/ReadProgressCallback.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/ToMainPortTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include "Processors/Transforms/TableFinishTransform.h"

#include <sys/eventfd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryPipeline::checkInitialized()
{
    if (!initialized())
        throw Exception("QueryPipeline wasn't initialized.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::checkInitializedAndNotCompleted()
{
    checkInitialized();

    if (pipe.isCompleted())
        throw Exception("QueryPipeline was already completed.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSource(const ProcessorPtr & source, bool can_have_totals)
{
    if (!source->getInputs().empty())
        throw Exception("Source for query pipeline shouldn't have any input, but " + source->getName() + " has " +
                        toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().empty())
        throw Exception("Source for query pipeline should have single output, but it doesn't have any",
                ErrorCodes::LOGICAL_ERROR);

    if (!can_have_totals && source->getOutputs().size() != 1)
        throw Exception("Source for query pipeline should have single output, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().size() > 2)
        throw Exception("Source for query pipeline should have 1 or 2 outputs, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::init(Pipe pipe_)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized.", ErrorCodes::LOGICAL_ERROR);

    if (pipe_.empty())
        throw Exception("Can't initialize pipeline with empty pipe.", ErrorCodes::LOGICAL_ERROR);

    pipe = std::move(pipe_);
}

void QueryPipeline::reset()
{
    Pipe pipe_to_destroy(std::move(pipe));
    *this = QueryPipeline();
}

void QueryPipeline::addSimpleTransform(const Pipe::ProcessorGetter & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipeline::addSimpleTransform(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipeline::addTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();
    pipe.addTransform(std::move(transform));
}

void QueryPipeline::addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes)
{
    checkInitializedAndNotCompleted();
    pipe.addTransform(std::move(transform), totals, extremes);
}

void QueryPipeline::transform(const Transformer & transformer, size_t sink_num)
{
    checkInitializedAndNotCompleted();
    pipe.transform(transformer, sink_num);
}

void QueryPipeline::setSinks(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.setSinks(getter);
}

void QueryPipeline::addDelayedStream(ProcessorPtr source)
{
    checkInitializedAndNotCompleted();

    checkSource(source, false);
    assertBlocksHaveEqualStructure(getHeader(), source->getOutputs().front().getHeader(), "QueryPipeline");

    IProcessor::PortNumbers delayed_streams = { pipe.numOutputPorts() };
    pipe.addSource(std::move(source));

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts(), delayed_streams);
    addTransform(std::move(processor));
}

void QueryPipeline::addMergingAggregatedMemoryEfficientTransform(AggregatingTransformParamsPtr params, size_t num_merging_processors)
{
    DB::addMergingAggregatedMemoryEfficientTransform(pipe, std::move(params), num_merging_processors);
}

void QueryPipeline::resize(size_t num_streams, bool force, bool strict)
{
    checkInitializedAndNotCompleted();
    pipe.resize(num_streams, force, strict);
}

void QueryPipeline::addTotalsHavingTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();

    if (!typeid_cast<const TotalsHavingTransform *>(transform.get()))
        throw Exception("TotalsHavingTransform expected for QueryPipeline::addTotalsHavingTransform.",
                ErrorCodes::LOGICAL_ERROR);

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    resize(1);

    auto * totals_port = &transform->getOutputs().back();
    pipe.addTransform(std::move(transform), totals_port, nullptr);
}

void QueryPipeline::addDefaultTotals()
{
    checkInitializedAndNotCompleted();

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    const auto & current_header = getHeader();
    Columns columns;
    columns.reserve(current_header.columns());

    for (size_t i = 0; i < current_header.columns(); ++i)
    {
        auto column = current_header.getByPosition(i).type->createColumn();
        column->insertDefault();
        columns.emplace_back(std::move(column));
    }

    auto source = std::make_shared<SourceFromSingleChunk>(current_header, Chunk(std::move(columns), 1));
    pipe.addTotalsSource(std::move(source));
}

void QueryPipeline::setTotalsPortToMainPortTransform()
{
    if (pipe.getTotalsPort())
    {
        checkInitializedAndNotCompleted();
        auto transform = std::make_shared<TotalsPortToMainPortTransform>(getHeader());
        pipe.addSimpleTransformToPort(transform, pipe.getTotalsPort());
    }
}

void QueryPipeline::setExtremesPortToMainPortTransform()
{
    if (pipe.getExtremesPort())
    {
        checkInitializedAndNotCompleted();
        auto transform = std::make_shared<ExtremesPortToMainPortTransform>(getHeader());
        pipe.addSimpleTransformToPort(transform, pipe.getExtremesPort());
    }
}

void QueryPipeline::dropTotalsAndExtremes()
{
    pipe.dropTotals();
    pipe.dropExtremes();
}

void QueryPipeline::addExtremesTransform()
{
    checkInitializedAndNotCompleted();

    /// It is possible that pipeline already have extremes.
    /// For example, it may be added from VIEW subquery.
    /// In this case, recalculate extremes again - they should be calculated for different rows.
    if (pipe.getExtremesPort())
        pipe.dropExtremes();

    resize(1);
    auto transform = std::make_shared<ExtremesTransform>(getHeader());
    auto * port = &transform->getExtremesPort();
    pipe.addTransform(std::move(transform), nullptr, port);
}

void QueryPipeline::setOutputFormat(ProcessorPtr output)
{
    checkInitializedAndNotCompleted();

    if (output_format)
        throw Exception("QueryPipeline already has output.", ErrorCodes::LOGICAL_ERROR);

    resize(1);

    output_format = dynamic_cast<IOutputFormat * >(output.get());
    pipe.setOutputFormat(std::move(output));

    initRowsBeforeLimit();
}

QueryPipeline QueryPipeline::unitePipelines(
    std::vector<std::unique_ptr<QueryPipeline>> pipelines,
    size_t max_threads_limit,
    Processors * collected_processors)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of pipelines");

    Block common_header = pipelines.front()->getHeader();

    /// Should we limit the number of threads for united pipeline. True if all pipelines have max_threads != 0.
    /// If true, result max_threads will be sum(max_threads).
    /// Note: it may be > than settings.max_threads, so we should apply this limit again.
    bool will_limit_max_threads = true;
    size_t max_threads = 0;
    size_t min_threads = 0;
    Pipes pipes;

    for (auto & pipeline_ptr : pipelines)
    {
        auto & pipeline = *pipeline_ptr;
        pipeline.checkInitialized();
        pipeline.pipe.collected_processors = collected_processors;

        pipes.emplace_back(std::move(pipeline.pipe));

        max_threads += pipeline.max_threads;
        min_threads += pipeline.min_threads;
        will_limit_max_threads = will_limit_max_threads && pipeline.max_threads != 0;

        /// If one of pipelines uses more threads then current limit, will keep it.
        /// It may happen if max_distributed_connections > max_threads
        if (pipeline.max_threads > max_threads_limit)
            max_threads_limit = pipeline.max_threads;
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes), collected_processors, false));

    if (will_limit_max_threads)
    {
        pipeline.setMaxThreads(max_threads);
        pipeline.limitMaxThreads(max_threads_limit);
    }
    pipeline.limitMinThreads(min_threads);

    return pipeline;
}

std::unique_ptr<QueryPipeline> QueryPipeline::joinPipelines(
    std::unique_ptr<QueryPipeline> left,
    std::unique_ptr<QueryPipeline> right,
    JoinPtr join,
    size_t max_block_size,
    size_t max_streams,
    bool keep_left_read_in_order,
    bool join_parallel_left_right,
    Processors * collected_processors,
    bool need_build_runtime_filter)
{
    left->checkInitializedAndNotCompleted();
    right->checkInitializedAndNotCompleted();

    /// Extremes before join are useless. They will be calculated after if needed.
    left->pipe.dropExtremes();
    right->pipe.dropExtremes();

    left->pipe.collected_processors = collected_processors;
    right->pipe.collected_processors = collected_processors;

    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool default_totals = false;
    if (!left->hasTotals() && right->hasTotals())
    {
        left->addDefaultTotals();
        default_totals = true;
    }

    ///                                     (left) ──────┐
    ///                                                  ╞> Joining ─> (joined)
    ///                                     (left) ─┐┌───┘
    ///                                             └┼───┐
    /// (right) ┐                         (totals) ──┼─┐ ╞> Joining ─> (joined)
    ///         ╞> Resize ┐                        ╓─┘┌┼─┘
    /// (right) ┘         │                        ╟──┘└─┐
    ///                   ╞> FillingJoin ─> Resize ╣     ╞> Joining ─> (totals)
    /// (totals) ─────────┘                        ╙─────┘

    size_t num_streams = left->getNumStreams();

    if (join->supportParallelJoin() && !right->hasTotals())
    {
        if (!keep_left_read_in_order)
        {
            left->resize(max_streams);
            num_streams = max_streams;
        }

        right->resize(max_streams);
        std::shared_ptr<JoiningTransform::FinishCounter> finish_counter;
        if (need_build_runtime_filter)
            finish_counter = std::make_shared<JoiningTransform::FinishCounter>(max_streams);

        auto concurrent_right_filling_transform = [&](OutputPortRawPtrs outports)
        {
            Processors processors;
            for (auto & outport : outports)
            {
                auto adding_joined = std::make_shared<FillingRightJoinSideTransform>(right->getHeader(), join, finish_counter);
                connect(*outport, adding_joined->getInputs().front());
                processors.emplace_back(adding_joined);
            }
            return processors;
        };
        right->transform(concurrent_right_filling_transform);
        right->resize(1);
    }
    else
    {
        right->resize(1);
        std::shared_ptr<JoiningTransform::FinishCounter> finish_counter;
        if (need_build_runtime_filter)
            finish_counter = std::make_shared<JoiningTransform::FinishCounter>(1);

        auto adding_joined = std::make_shared<FillingRightJoinSideTransform>(right->getHeader(), join, finish_counter);
        InputPort * totals_port = nullptr;
        if (right->hasTotals())
            totals_port = adding_joined->addTotalsPort();

        right->addTransform(std::move(adding_joined), totals_port, nullptr);
    }

    size_t num_streams_including_totals = num_streams + (left->hasTotals() ? 1 : 0);
    right->resize(num_streams_including_totals);

    /// This counter is needed for every Joining except totals, to decide which Joining will generate non joined rows.
    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(num_streams);
    auto finish_pipe = std::make_shared<std::vector<JoiningTransform::EventFdStruct>>();
    if (join->getType() == JoinType::PARALLEL_HASH)
    {
        for (size_t i = 0; i < num_streams; i++)
        {
            finish_pipe->push_back({-1});
        }

        for (size_t i = 0; i < num_streams; i++)
        {
            int event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (-1 == event_fd)
                throwFromErrno("Cannot create event_fd for ConcurrentJoin", ErrorCodes::CANNOT_OPEN_FILE);
            (*finish_pipe)[i].event_fd = event_fd;
        }
    }

    auto lit = left->pipe.output_ports.begin();
    auto rit = right->pipe.output_ports.begin();

    std::vector<OutputPort *> joined_output_ports;
    std::vector<OutputPort *> delayed_root_output_ports;

    std::shared_ptr<DelayedJoinedBlocksTransform> delayed_root = nullptr;
    if (join->hasDelayedBlocks())
    {
        delayed_root = std::make_shared<DelayedJoinedBlocksTransform>(num_streams, join);
        if (!delayed_root->getInputs().empty() || delayed_root->getOutputs().size() != num_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "DelayedJoinedBlocksTransform should have no inputs and {} outputs, "
                            "but has {} inputs and {} outputs",
                            num_streams, delayed_root->getInputs().size(), delayed_root->getOutputs().size());

        if (collected_processors)
            collected_processors->emplace_back(delayed_root);
        left->pipe.processors.emplace_back(delayed_root);

        for (auto & outport : delayed_root->getOutputs())
            delayed_root_output_ports.emplace_back(&outport);
    }


    Block left_header = left->getHeader();
    Block joined_header = JoiningTransform::transformHeader(left_header, join);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto joining = std::make_shared<JoiningTransform>(left->getHeader(), join, max_block_size, false, default_totals, join_parallel_left_right,finish_counter, num_streams, i, finish_pipe);
        connect(**lit, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        if (delayed_root)
        {
            // Process delayed joined blocks when all JoiningTransform are finished.
            auto delayed = std::make_shared<DelayedJoinedBlocksWorkerTransform>(left_header, joined_header, max_block_size, join);
            if (delayed->getInputs().size() != 1 || delayed->getOutputs().size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform should have one input and one output");

            connect(*delayed_root_output_ports[i], delayed->getInputs().front());

            joined_output_ports.push_back(&joining->getOutputs().front());
            joined_output_ports.push_back(&delayed->getOutputs().front());

            if (collected_processors)
                collected_processors->emplace_back(delayed);
            left->pipe.processors.emplace_back(std::move(delayed));
        }
        else
        {
            *lit = &joining->getOutputs().front();
        }

        ++lit;
        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors.emplace_back(std::move(joining));
    }

    if (delayed_root)
    {
        // Process DelayedJoinedBlocksTransform after all JoiningTransforms.
        DelayedPortsProcessor::PortNumbers delayed_ports_numbers;
        delayed_ports_numbers.reserve(joined_output_ports.size() / 2);
        for (size_t i = 1; i < joined_output_ports.size(); i += 2)
            delayed_ports_numbers.push_back(i);

        auto delayed_processor = std::make_shared<DelayedPortsProcessor>(joined_header, 2 * num_streams, delayed_ports_numbers);
        if (collected_processors)
            collected_processors->emplace_back(delayed_processor);
        left->pipe.processors.emplace_back(delayed_processor);

        // Connect @delayed_processor ports with inputs (JoiningTransforms & DelayedJoinedBlocksTransforms) / pipe outputs
        auto next_delayed_input = delayed_processor->getInputs().begin();
        for (OutputPort * port : joined_output_ports)
            connect(*port, *next_delayed_input++);
        left->pipe.output_ports.clear();
        for (OutputPort & port : delayed_processor->getOutputs())
            left->pipe.output_ports.push_back(&port);
        left->pipe.header = joined_header;
        left->resize(num_streams);
    }

    if (left->hasTotals())
    {
        auto joining = std::make_shared<JoiningTransform>(left->getHeader(), join, max_block_size, true, default_totals, join_parallel_left_right);
        connect(*left->pipe.totals_port, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        left->pipe.totals_port = &joining->getOutputs().front();

        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors.emplace_back(std::move(joining));
    }

    left->pipe.processors.insert(left->pipe.processors.end(), right->pipe.processors.begin(), right->pipe.processors.end());
    left->pipe.holder = std::move(right->pipe.holder);
    left->pipe.header = left->pipe.output_ports.front()->getHeader();
    left->pipe.max_parallel_streams = std::max(left->pipe.max_parallel_streams, right->pipe.max_parallel_streams);
    left->min_threads = std::max(left->min_threads, right->min_threads);
    return left;
}

void QueryPipeline::addCreatingSetsTransform(const Block & res_header, SubqueryForSet subquery_for_set, const SizeLimits & limits, ContextPtr context)
{
    resize(1);

    auto transform = std::make_shared<CreatingSetsTransform>(
            getHeader(),
            res_header,
            std::move(subquery_for_set),
            limits,
            context);

    InputPort * totals_port = nullptr;

    if (pipe.getTotalsPort())
        totals_port = transform->addTotalsPort();

    pipe.addTransform(std::move(transform), totals_port, nullptr);
}

void QueryPipeline::addPipelineBefore(QueryPipeline pipeline)
{
    checkInitializedAndNotCompleted();
    if (pipeline.getHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for CreatingSets should have empty header. Got: {}",
                        pipeline.getHeader().dumpStructure());

    IProcessor::PortNumbers delayed_streams(pipe.numOutputPorts());
    for (size_t i = 0; i < delayed_streams.size(); ++i)
        delayed_streams[i] = i;

    auto * collected_processors = pipe.collected_processors;

    Pipes pipes;
    pipes.emplace_back(std::move(pipe));
    pipes.emplace_back(QueryPipeline::getPipe(std::move(pipeline)));
    pipe = Pipe::unitePipes(std::move(pipes), collected_processors, true);

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts(), delayed_streams, true);
    addTransform(std::move(processor));
}

void QueryPipeline::setProgressCallback(const ProgressCallback & callback)
{
    progress_callback = callback;
    for (auto & processor : pipe.processors)
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProgressCallback(callback);
        if (auto * finish_transform = dynamic_cast<TableFinishTransform *>(processor.get()))
            finish_transform->setProgressCallback(callback);
    }
}

std::unique_ptr<ReadProgressCallback> QueryPipeline::getReadProgressCallback() const
{
    auto callback = std::make_unique<ReadProgressCallback>();

    callback->setProgressCallback(progress_callback);
    callback->setQuota(quota);
    callback->setProcessListElement(process_list_element);

    if (!update_profile_events)
        callback->disableProfileEventUpdate();

    return callback;
}

void QueryPipeline::setProcessListElement(QueryStatus * elem)
{
    process_list_element = elem;

    for (auto & processor : pipe.processors)
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProcessListElement(elem);
        if (auto * finish_transform = dynamic_cast<TableFinishTransform *>(processor.get()))
            finish_transform->setProcessListElement(elem);
    }
}

void QueryPipeline::initRowsBeforeLimit()
{
    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// TODO: add setRowsBeforeLimitCounter as virtual method to IProcessor.
    std::vector<LimitTransform *> limits;
    std::vector<SourceFromInputStream *> sources;
    std::vector<RemoteSource *> remote_sources;

    std::unordered_set<IProcessor *> visited;

    struct QueuedEntry
    {
        IProcessor * processor;
        bool visited_limit;
    };

    std::queue<QueuedEntry> queue;

    queue.push({ output_format, false });
    visited.emplace(output_format);

    while (!queue.empty())
    {
        auto * processor = queue.front().processor;
        auto visited_limit = queue.front().visited_limit;
        queue.pop();

        if (!visited_limit)
        {
            if (auto * limit = typeid_cast<LimitTransform *>(processor))
            {
                visited_limit = true;
                limits.emplace_back(limit);
            }

            if (auto * source = typeid_cast<SourceFromInputStream *>(processor))
                sources.emplace_back(source);

            if (auto * source = typeid_cast<RemoteSource *>(processor))
                remote_sources.emplace_back(source);
        }
        else if (auto * sorting = typeid_cast<PartialSortingTransform *>(processor))
        {
            if (!rows_before_limit_at_least)
                rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

            sorting->setRowsBeforeLimitCounter(rows_before_limit_at_least);

            /// Don't go to children. Take rows_before_limit from last PartialSortingTransform.
            continue;
        }

        /// Skip totals and extremes port for output format.
        if (auto * format = dynamic_cast<IOutputFormat *>(processor))
        {
            auto * child_processor = &format->getPort(IOutputFormat::PortKind::Main).getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });

            continue;
        }

        for (auto & child_port : processor->getInputs())
        {
            auto * child_processor = &child_port.getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });
        }
    }

    if (!rows_before_limit_at_least && (!limits.empty() || !sources.empty() || !remote_sources.empty()))
    {
        rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

        for (auto & limit : limits)
            limit->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : remote_sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);
    }

    /// If there is a limit, then enable rows_before_limit_at_least
    /// It is needed when zero rows is read, but we still want rows_before_limit_at_least in result.
    if (!limits.empty())
        rows_before_limit_at_least->add(0);

    if (rows_before_limit_at_least)
        output_format->setRowsBeforeLimitCounter(rows_before_limit_at_least);
}

PipelineExecutorPtr QueryPipeline::execute()
{
    if (!isCompleted())
        throw Exception("Cannot execute pipeline because it is not completed.", ErrorCodes::LOGICAL_ERROR);

    return std::make_shared<PipelineExecutor>(pipe.processors, process_list_element);
}

static void addMaterializing(OutputPort *& output, Processors & processors)
{
    if (!output)
        return;

    auto materializing = std::make_shared<MaterializingTransform>(output->getHeader());
    connect(*output, materializing->getInputPort());
    output = &materializing->getOutputPort();
    processors.emplace_back(std::move(materializing));
}

void QueryPipeline::complete(std::shared_ptr<IOutputFormat> format)
{
    auto & output = pipe.output_ports[0];
    auto & totals = pipe.totals_port;
    auto & extremes = pipe.extremes_port;

    if (format->expectMaterializedColumns())
    {
        addMaterializing(output, pipe.processors);
        addMaterializing(totals, pipe.processors);
        addMaterializing(extremes, pipe.processors);
    }

    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(format_totals.getHeader());
        totals = &source->getPort();
        pipe.processors.emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(format_extremes.getHeader());
        extremes = &source->getPort();
        pipe.processors.emplace_back(std::move(source));
    }

    connect(*output, format_main);
    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    output = nullptr;
    totals = nullptr;
    extremes = nullptr;
    pipe.output_ports.clear();

    output_format = format.get();

    pipe.processors.emplace_back(std::move(format));
}

void QueryPipeline::setCollectedProcessors(Processors * processors)
{
    pipe.collected_processors = processors;
}

const CacheHolderPtr QueryPipeline::getCacheHolder() const
{
    return pipe.holder.cache_holder;
}

void QueryPipeline::setWriteCacheComplete(const ContextPtr & context)
{
    if (!pipe.holder.cache_holder)
        return;

    auto cache = context->getIntermediateResultCache();
    auto & write_cache = pipe.holder.cache_holder->write_cache;
    for (auto cache_key : write_cache)
        cache->setComplete(cache_key);
    write_cache.clear();
}

void QueryPipeline::clearUncompletedCache(const ContextPtr & context)
{
    if (!pipe.holder.cache_holder)
        return;

    auto cache = context->getIntermediateResultCache();
    auto & write_cache = pipe.holder.cache_holder->write_cache;
    for (auto cache_key : write_cache)
        cache->eraseUncompletedCache(cache_key);
}

QueryPipelineProcessorsCollector::QueryPipelineProcessorsCollector(QueryPipeline & pipeline_, IQueryPlanStep * step_)
    : pipeline(pipeline_), step(step_)
{
    pipeline.setCollectedProcessors(&processors);
}

QueryPipelineProcessorsCollector::~QueryPipelineProcessorsCollector()
{
    pipeline.setCollectedProcessors(nullptr);
}

Processors QueryPipelineProcessorsCollector::detachProcessors(size_t group)
{
    for (auto & processor : processors)
        processor->setQueryPlanStep(step, group);

    Processors res;
    res.swap(processors);
    return res;
}

void QueryPipeline::writeResultIntoQueryCache(std::shared_ptr<QueryCache::Writer> query_cache_writer)
{
    assert(getNumStreams() > 0);
    pipe.addQueryCacheTransform(std::move(query_cache_writer));
}

void QueryPipeline::finalizeWriteInQueryCache()
{
    pipe.finalizeWriteInQueryCache();
}

void QueryPipeline::readFromQueryCache(
        std::unique_ptr<SourceFromChunks> source,
        std::unique_ptr<SourceFromChunks> source_totals,
        std::unique_ptr<SourceFromChunks> source_extremes)
{
    pipe.readFromQueryCache(std::move(source), std::move(source_totals), std::move(source_extremes));
}

void QueryPipeline::addUsedStorageIDs(const std::set<StorageID> & used_storage_ids_)
{
    used_storage_ids.insert(used_storage_ids_.begin(), used_storage_ids_.end());
}

std::set<StorageID> QueryPipeline::getUsedStorageIDs() const
{
    return used_storage_ids;
}

bool QueryPipeline::hasAllUsedStorageIDs() const
{
    return has_all_used_storage_ids;
}

void QueryPipeline::setHasAllUsedStorageIDs(bool val)
{
    has_all_used_storage_ids = val;
}

}

