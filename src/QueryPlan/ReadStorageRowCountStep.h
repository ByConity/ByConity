#pragma once
#include <utility>
#include <QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ExpressionActions.h>
#include "common/types.h"

namespace DB
{

class ReadStorageRowCountStep : public ISourceStep
{
public:
    explicit ReadStorageRowCountStep(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, bool is_final_agg_, StorageID storage_id_);
    
    String getName() const override { return "ReadStorageRowCount"; }

    Type getType() const override { return Type::ReadStorageRowCount; }
    
    AggregateDescription getAggregateDescription() const { return agg_desc; }

    ASTPtr getQuery() const { return query; }

    StorageID getStorageID() const { return storage_id; }

    void setNumRows(UInt64 num_rows_) { num_rows = num_rows_; }

    UInt64 getNumRows() const { return num_rows; }

    bool isFinal() const { return is_final_agg; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void toProto(Protos::ReadStorageRowCountStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ReadStorageRowCountStep> fromProto(const Protos::ReadStorageRowCountStep & proto, ContextPtr context);

private:
    ASTPtr query;
    AggregateDescription agg_desc;
    std::shared_ptr<Cluster> optimized_cluster;
    UInt64 num_rows;
    bool is_final_agg;
    StorageID storage_id;
};

}
