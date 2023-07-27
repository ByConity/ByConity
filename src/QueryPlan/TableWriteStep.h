#pragma once
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

    TableWriteStep(const DataStream & input_stream_, TargetPtr target_);

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

    void serialize(WriteBuffer & buffer) const override;

    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr & context);

private:
    Block getHeader(const NamesAndTypes & input_columns);

    BlockOutputStreams createOutputStream(
        StoragePtr target_table,
        const BuildQueryPipelineSettings & settings,
        Block header,
        size_t max_threads,
        bool no_destination,
        bool no_squash);

    TargetPtr target;
};

class TableWriteStep::Target
{
public:
    virtual ~Target() = default;
    virtual TargetType getTargeType() const = 0;
    virtual void serializeImpl(WriteBuffer &) const = 0;
    virtual String toString() const = 0;
    virtual StoragePtr getStorage() const = 0;
    virtual NameToNameMap getTableColumnToInputColumnMap(const Names & input_columns) const = 0;

    void serialize(WriteBuffer &) const;
    static TargetPtr deserialize(ReadBuffer &, ContextPtr &);
};

class TableWriteStep::InsertTarget : public TableWriteStep::Target
{
public:
    InsertTarget(StoragePtr storage_, StorageID storage_id_, NamesAndTypes columns_)
        : storage(std::move(storage_)), storage_id(storage_id_), columns(std::move(columns_))
    {
    }

    TargetType getTargeType() const override
    {
        return TargetType::INSERT;
    }
    void serializeImpl(WriteBuffer &) const override;
    String toString() const override;
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

    static std::shared_ptr<InsertTarget> deserialize(ReadBuffer &, ContextPtr &);

private:
    StoragePtr storage;
    StorageID storage_id;
    NamesAndTypes columns;
};

}
