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
    ReadFromStorageStep(Pipe pipe_, String storage_name, StorageID storage_id_ = StorageID::createEmpty())
        : ReadFromPreparedSource(std::move(pipe_)), storage_id(storage_id_)
    {
        setStepDescription(storage_name);
    }

    String getName() const override { return "ReadFromStorage"; }

    Type getType() const override { return Type::ReadFromStorage; }

    void serialize(WriteBuffer &) const override;

    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

    /**
     * Information for deserialize and reconstruct the plan.
     */
    void setDeserializeInfo(
        const SelectQueryInfo & query_info_,
        const Names & column_names_,
        QueryProcessingStage::Enum processed_stage_,
        size_t max_block_size_,
        unsigned num_streams_
    ){
        query_info = query_info_;
        column_names = column_names_;
        processed_stage = processed_stage_;
        max_block_size = max_block_size_;
        num_streams = num_streams_;
    }

    StorageID storage_id;
    SelectQueryInfo query_info;
    Names column_names;
    QueryProcessingStage::Enum processed_stage;
    size_t max_block_size;
    unsigned num_streams;
};

}
