#pragma once
#include <QueryPlan/SetOperationStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public SetOperationStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStep(
        DataStreams input_streams_,
        DataStream output_stream_,
        std::unordered_map<String, std::vector<String>> output_to_inputs_,
        size_t max_threads_ = 0,
        bool local_ = false);

    explicit UnionStep(DataStreams input_streams_, size_t max_threads_ = 0, bool local_ = false) : UnionStep(input_streams_, DataStream{}, {}, max_threads_, local_) { }
    UnionStep(DataStreams input_streams_, DataStream output_stream_, size_t max_threads_ = 0, bool local_ = false) : UnionStep(input_streams_, output_stream_, {}, max_threads_, local_) { }

    String getName() const override { return "Union"; }

    Type getType() const override { return Type::Union; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }
    bool isLocal() const { return local; }

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;

private:
    Block header;
    size_t max_threads;
    bool local;
    Processors processors;
};

}
