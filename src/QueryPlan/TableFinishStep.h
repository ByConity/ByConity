#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/TableWriteStep.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{
class TableFinishStep : public ITransformingStep
{
public:
    TableFinishStep(const DataStream & input_stream_, TableWriteStep::TargetPtr target_, String output_affected_row_count_symbol_, ASTPtr query_, bool insert_select_with_profiles_ = false);

    String getName() const override
    {
        return "TableFinish";
    }
    Type getType() const override
    {
        return Type::TableFinish;
    }
    
    void preExecute(ContextMutablePtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;
    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        if (insert_select_with_profiles)
        {
            Block new_header = {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), output_affected_row_count_symbol)};
            output_stream = DataStream{.header = std::move(new_header)};
        }
        else
            output_stream = DataStream{.header = std::move((input_streams_[0].header))};
    }

    TableWriteStep::TargetPtr getTarget() const
    {
        return target;
    }

    const String & getOutputAffectedRowCountSymbol() const { return output_affected_row_count_symbol; }

    void setQuery(const ASTPtr & query_) { query = query_; }
    ASTPtr getQuery() const { return query; }

    bool isOutputProfiles() const { return insert_select_with_profiles; }

    void toProto(Protos::TableFinishStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<TableFinishStep> fromProto(const Protos::TableFinishStep & proto, ContextPtr context);

private:
    TableWriteStep::TargetPtr target;
    String output_affected_row_count_symbol;
    ASTPtr query;
    bool insert_select_with_profiles;
    LoggerPtr log;
};
}
