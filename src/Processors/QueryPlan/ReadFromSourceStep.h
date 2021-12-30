#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>
#include <Core/QueryProcessingStage.h>

namespace DB
{

class ReadFromSourceStep : public ISourceStep
{
public:
    explicit ReadFromSourceStep(Block header_,
                                StorageID storage_id_, 
                                const SelectQueryInfo & query_info_,
                                const Names & column_names_,
                                QueryProcessingStage::Enum processed_stage_,
                                size_t max_block_size_,
                                unsigned num_streams_,
                                ContextPtr context_ = nullptr);

    String getName() const override { return "ReadFromSourceStep"; }

    Type getType() const override { return Type::ReadFromSource; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer &) const override;

    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

    QueryPlanStepPtr generateStep();

    StorageID getStorageID() const { return storage_id; }

private:

    StorageID storage_id;
    SelectQueryInfo query_info;
    Names column_names;
    QueryProcessingStage::Enum processed_stage;
    size_t max_block_size;
    unsigned num_streams;

    ContextPtr context;
};

}
