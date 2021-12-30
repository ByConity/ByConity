#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/StorageID.h>
#include <Core/QueryProcessingStage.h>

namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_, ContextPtr context_ = nullptr);

    String getName() const override { return "ReadFromPreparedSource"; }

    Type getType() const override { return Type::ReadFromPreparedSource; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

private:
    Pipe pipe;
    ContextPtr context;
};

class ReadFromStorageStep : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, String storage_name)
        : ReadFromPreparedSource(std::move(pipe_))
    {
        setStepDescription(storage_name);
    }

    String getName() const override { return "ReadFromStorage"; }

    Type getType() const override { return Type::ReadFromStorage; }

    // void serialize(WriteBuffer &) const override;

    // static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

};

}
