#pragma once
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
    explicit ReadStorageRowCountStep(Block output_header, StorageID storage_id_, ASTPtr query_, AggregateDescription agg_desc_, UInt64 num_rows_);
    
    String getName() const override { return "ReadStorageRowCount"; }

    Type getType() const override { return Type::ReadStorageRowCount; }

    StorageID getStorageID() const { return storage_id; }
    
    AggregateDescription getAggregateDescription() const { return agg_desc; }

    ASTPtr getQuery() const { return query; }

    UInt64 getNumRows() const { return num_rows; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void toProto(Protos::ReadStorageRowCountStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ReadStorageRowCountStep> fromProto(const Protos::ReadStorageRowCountStep & proto, ContextPtr context);

    std::shared_ptr<Cluster> getOptimizedCluster() { return optimized_cluster; }

private:
    StorageID storage_id;
    ASTPtr query;
    AggregateDescription agg_desc;
    std::shared_ptr<Cluster> optimized_cluster;
    UInt64 num_rows;
};

}
