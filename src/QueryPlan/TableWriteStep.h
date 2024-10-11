#pragma once
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

class TableWriteStep : public ITransformingStep
{
public:
    class Target;
    using TargetPtr = std::shared_ptr<Target>;
    class InsertTarget;

    enum class TargetType : UInt8
    {
        INSERT,
    };

    TableWriteStep(
        const DataStream & input_stream_, TargetPtr target_, bool insert_select_with_profiles_, String output_affected_row_count_symbol_);

    String getName() const override
    {
        return "TableWrite";
    }

    Type getType() const override
    {
        return Type::TableWrite;
    }

    const TargetPtr & getTarget() const
    {
        return target;
    }

    void setInputStreams(const DataStreams & input_streams_) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void allocate(const ContextPtr & context);

    bool isOutputProfiles() const { return insert_select_with_profiles; }

    const String & getOutputAffectedRowCountSymbol() const { return output_affected_row_count_symbol; }

    void toProto(Protos::TableWriteStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<TableWriteStep> fromProto(const Protos::TableWriteStep & proto, ContextPtr context);

private:
    Block getHeader(const NamesAndTypes & input_columns);

    BlockOutputStreams createOutputStream(
        StoragePtr target_table,
        const BuildQueryPipelineSettings & settings,
        Block & header,
        size_t max_threads,
        bool no_destination,
        ASTPtr query);

    TargetPtr target;
    bool insert_select_with_profiles;
    String output_affected_row_count_symbol;
};

class TableWriteStep::Target
{
public:
    virtual ~Target() = default;
    virtual TargetType getTargetType() const = 0;
    String toString() const
    {
        return toString({});
    }
    virtual String toString(const String & remove_tenant_id) const = 0;
    virtual StoragePtr getStorage() const = 0;
    virtual NameToNameMap getTableColumnToInputColumnMap(const Names & input_columns) const = 0;

    void toProto(Protos::TableWriteStep::Target & proto) const;
    static TargetPtr fromProto(const Protos::TableWriteStep::Target & proto, ContextPtr context);
};

class TableWriteStep::InsertTarget : public TableWriteStep::Target
{
public:
    InsertTarget(StoragePtr storage_, StorageID storage_id_, NamesAndTypes columns_, ASTPtr query_)
        : storage(std::move(storage_)), storage_id(storage_id_), columns(std::move(columns_)), query(query_)
    {
    }

    TargetType getTargetType() const override { return TargetType::INSERT; }
    String toString(const String & remove_tenant_id) const override;
    StoragePtr getStorage() const override
    {
        return storage;
    }
    NameToNameMap getTableColumnToInputColumnMap(const Names & input_columns) const override;
    const NamesAndTypes & getColumns() const
    {
        return columns;
    }
    void setTable(const String & table_);
    StorageID getStorageID() const
    {
        return storage_id;
    }

    ASTPtr getQuery() const
    {
        return query;
    }

    void toProtoImpl(Protos::TableWriteStep::InsertTarget & proto) const;
    static std::shared_ptr<InsertTarget> createFromProtoImpl(const Protos::TableWriteStep::InsertTarget & proto, ContextPtr context);

private:
    StoragePtr storage;
    StorageID storage_id;
    NamesAndTypes columns;
    ASTPtr query;
};

}
