#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/NestedLoopJoin.h>
#include <Interpreters/JoinSwitcher.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
    };
}

QueryPipelinePtr JoinStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    return QueryPipeline::joinPipelines(std::move(pipelines[0]), std::move(pipelines[1]), join, max_block_size, &processors);
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::serialize(WriteBuffer & buf) const
{
    writeBinary(step_description, buf);

    if (input_streams.size() < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input streams");

    serializeDataStream(input_streams[0], buf);
    serializeDataStream(input_streams[1], buf);

    if (join)
    {
        writeBinary(true, buf);
        serializeEnum(join->getType(), buf);
        join->serialize(buf);
    }
    else
        writeBinary(false, buf);

    // todo serialize join

    writeBinary(max_block_size, buf);
}

QueryPlanStepPtr JoinStep::deserialize(ReadBuffer & buf, ContextPtr context)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream left_stream = deserializeDataStream(buf);
    DataStream right_stream = deserializeDataStream(buf);

    // todo deserialize join
    JoinPtr join = nullptr;
    bool has_join;
    readBinary(has_join, buf);
    if (has_join)
    {
        JoinType type;
        deserializeEnum(type, buf);
        switch(type)
        {
            case JoinType::Hash:
                join = HashJoin::deserialize(buf, context);
                break;
            case JoinType::Merge:
                join = MergeJoin::deserialize(buf, context);
                break;
            case JoinType::NestedLoop:
                join = NestedLoopJoin::deserialize(buf, context);
                break;
            case JoinType::Switcher:
                join = JoinSwitcher::deserialize(buf, context);
                break;
        }
    }

    size_t max_block_size;
    readBinary(max_block_size, buf);
    
    auto step = std::make_unique<JoinStep>(left_stream, right_stream, std::move(join), max_block_size);

    step->setStepDescription(step_description);
    return step;
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);

    // todo serialize join

    writeBinary(max_block_size, buf);
}

QueryPlanStepPtr FilledJoinStep::deserialize(ReadBuffer & buf, ContextPtr /*context*/)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream input_stream = deserializeDataStream(buf);

    // todo deserialize join
    JoinPtr join = nullptr;

    bool max_block_size;
    readBinary(max_block_size, buf);

    auto step = std::make_unique<FilledJoinStep>(input_stream, std::move(join), max_block_size);

    step->setStepDescription(step_description);
    return step;
}


}
