#include <memory>
#include <utility>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Poco/Logger.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/logger_useful.h>
#include "Functions/FunctionFactory.h"
#include "Interpreters/Context_fwd.h"

namespace DB
{
RepartitionTransform::RepartitionTransform(const Block & header_, size_t partition_num_, ColumnNumbers repartition_keys_, ExecutableFunctionPtr repartition_func_)
    : ISimpleTransform(header_, header_, true)
    , partition_num(partition_num_)
    , repartition_keys(std::move(repartition_keys_))
    , repartition_func(std::move(repartition_func_))
    , logger(&Poco::Logger::get("RepartitionTransform"))
{
}

IProcessor::Status RepartitionTransform::prepare()
{
    return ISimpleTransform::prepare();
}

void RepartitionTransform::transform(Chunk & chunk)
{
    IColumn::Selector partition_selector;
    RepartitionTransform::PartitionStartPoints partition_start_points;

    //TODO: support chunk info
    if(chunk.getChunkInfo()){
        LOG_WARNING(logger, "Chunk info {} will be overwrite by repartition", typeid(chunk.getChunkInfo().get()).name());
    }

    std::tie(partition_selector, partition_start_points)
        = doRepartition(partition_num, chunk, getInputPort().getHeader(), {}, repartition_func, REPARTITION_FUNC_RESULT_TYPE);
    ChunkInfoPtr repartion_info = std::make_shared<RepartitionChunkInfo>(
        std::move(partition_selector), std::move(partition_start_points), std::move(chunk.getChunkInfo()));
    chunk.setChunkInfo(std::move(repartion_info));
}

std::pair<IColumn::Selector, RepartitionTransform::PartitionStartPoints> RepartitionTransform::doRepartition(
    size_t partition_num,
    const Chunk & chunk,
    const Block & header,
    const ColumnNumbers & repartition_keys,
    ExecutableFunctionPtr repartition_func,
    const DataTypePtr & result_type)
{    
    size_t input_rows_count = chunk.getNumRows();
    auto selector_column = ColumnUInt64::create(input_rows_count);
    const Columns & columns = chunk.getColumns();


    ColumnsWithTypeAndName arguments;
    arguments.reserve(repartition_keys.size());
    for (size_t key_idx : repartition_keys)
    {
        const auto & type_and_name = header.getByPosition(key_idx);
        arguments.emplace_back(ColumnWithTypeAndName(columns[key_idx], type_and_name.type, type_and_name.name));
    }

    ColumnPtr hash_result = repartition_func->execute(arguments, result_type, input_rows_count, false);

    PartitionStartPoints partition_row_idx_start_points(partition_num + 1, 0);

    IColumn::Selector repartition_selector(input_rows_count, 0);
    PODArrayWithStackMemory<UInt32, 32> partition_index(input_rows_count, 0);

    for (size_t i = 0; i < input_rows_count; ++i)
        partition_index[i] = hash_result->get64(i) % partition_num;

    for (size_t i = 0; i < input_rows_count; ++i)
        partition_row_idx_start_points[partition_index[i]]++;

    // make partition_row_idx_start_points[partition_num] = input_rows_count
    for (size_t i = 1; i <= partition_num; ++i)
    {
        partition_row_idx_start_points[i] += partition_row_idx_start_points[i - 1];
    }

    for (size_t i = input_rows_count; i-- > 0;)
    {
        repartition_selector[partition_row_idx_start_points[partition_index[i]] - 1] = i;
        partition_row_idx_start_points[partition_index[i]]--;
    }
    return std::make_pair(std::move(repartition_selector), std::move(partition_row_idx_start_points));
}

ExecutableFunctionPtr RepartitionTransform::getDefaultRepartitionFunction(const ColumnsWithTypeAndName & arguments, ContextPtr context)
{
    FunctionOverloadResolverPtr func_builder = FunctionFactory::instance().get(REPARTITION_FUNC, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    return function_base->prepare(arguments);
}

const DataTypePtr RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE = std::make_shared<DataTypeUInt64>();

}
