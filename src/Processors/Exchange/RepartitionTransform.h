#pragma once
#include <tuple>
#include <utility>
#include <vector>
#include <Columns/FilterDescription.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Poco/Logger.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class RepartitionTransform : public ISimpleTransform
{
public:
    using PartitionStartPoints = std::vector<size_t>;

    struct RepartitionChunkInfo : public ChunkInfo
    {
        RepartitionChunkInfo(IColumn::Selector selector_, PartitionStartPoints start_points_, ChunkInfoPtr origin_chunk_info_)
            : selector(std::move(selector_)), start_points(std::move(start_points_)), origin_chunk_info(std::move(origin_chunk_info_))
        {
        }
        IColumn::Selector selector;
        PartitionStartPoints start_points;
        ChunkInfoPtr origin_chunk_info;
    };

    RepartitionTransform(
        const Block & header_, size_t partition_num_, ColumnNumbers repartition_keys_, ExecutableFunctionPtr repartition_func_);

    String getName() const override { return "RepartitionTransform"; }

    inline static const String REPARTITION_FUNC{"cityHash64"};

    static const DataTypePtr REPARTITION_FUNC_RESULT_TYPE;

    static std::pair<IColumn::Selector, PartitionStartPoints> doRepartition(
        size_t partition_num,
        const Chunk & chunk,
        const Block & header,
        const ColumnNumbers & repartition_keys,
        ExecutableFunctionPtr repartition_func,
        const DataTypePtr & result_type);

    static ExecutableFunctionPtr getDefaultRepartitionFunction(const ColumnsWithTypeAndName & arguments, ContextPtr context);

protected:
    void transform(Chunk & chunk) override;

private:
    size_t partition_num;
    ColumnNumbers repartition_keys;
    ExecutableFunctionPtr repartition_func;
    Poco::Logger * logger;
};

}
