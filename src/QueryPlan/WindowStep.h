#pragma once
#include <QueryPlan/ITransformingStep.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    explicit WindowStep(const DataStream & input_stream_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> & window_functions_,
            bool need_sort_);

    WindowStep(const DataStream & input_stream_, const WindowDescription & window_description_, bool need_sort_);

    String getName() const override { return "Window"; }

    Type getType() const override { return Type::Window; }
    const WindowDescription & getWindow() const { return window_description; }
    const std::vector<WindowFunctionDescription> & getFunctions() const { return window_functions; }
    bool needSort() const { return need_sort; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    Block input_header;
    bool need_sort;
};

}
