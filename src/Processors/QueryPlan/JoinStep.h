#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// Join two data streams.
class JoinStep : public IQueryPlanStep
{
public:
    JoinStep(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        size_t max_block_size_);

    String getName() const override { return "Join"; }

    Type getType() const override { return Type::Join; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);

private:
    JoinPtr join;
    size_t max_block_size;
    Processors processors;
};

/// Special step for the case when Join is already filled.
/// For StorageJoin and Dictionary.
class FilledJoinStep : public ITransformingStep
{
public:
    FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_);

    String getName() const override { return "FilledJoin"; }

    Type getType() const override { return Type::FilledJoin; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);

private:
    JoinPtr join;
    size_t max_block_size;
};

}
