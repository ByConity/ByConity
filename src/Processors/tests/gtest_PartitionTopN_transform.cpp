#include <memory>
#include <vector>
#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/Transforms/PartitionTopNTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>

using namespace DB;
namespace UnitTest
{
TEST(PartitionTopNTransform, doPartitionTopNTest)
{
    ColumnsWithTypeAndName cols;

    auto partition_by_column = ColumnUInt64::create();
    auto order_by_column = ColumnUInt64::create();
    size_t rows = 100;
    for (size_t i = 0; i < rows; i++)
    {
        partition_by_column->insert(i % 2);
        order_by_column->insert(i);
    }

    cols.emplace_back(std::move(partition_by_column), std::make_shared<DataTypeUInt64>(), "a");
    cols.emplace_back(std::move(order_by_column), std::make_shared<DataTypeUInt64>(), "b");

    Block data_block = Block(cols);
    Block header = data_block.cloneEmpty();
    Chunk chunk1(data_block.mutateColumns(), rows);

    partition_by_column = ColumnUInt64::create();
    order_by_column = ColumnUInt64::create();
    rows = 100;
    for (size_t i = 0; i < rows; i++)
    {
        partition_by_column->insert(i % 2);
        order_by_column->insert(i);
    }

    cols = ColumnsWithTypeAndName{};
    cols.emplace_back(std::move(partition_by_column), std::make_shared<DataTypeUInt64>(), "a");
    cols.emplace_back(std::move(order_by_column), std::make_shared<DataTypeUInt64>(), "b");
    data_block = Block(cols);
    Chunk chunk2(data_block.mutateColumns(), rows);

    ColumnNumbers partition_by_column_numbers;
    partition_by_column_numbers.emplace_back(0);
    ColumnNumbers order_by_column_numbers;
    order_by_column_numbers.emplace_back(1);
    PartitionTopNTransform partition_transform{header, 2, partition_by_column_numbers, order_by_column_numbers, PartitionTopNModel::RowNumber, true};
    partition_transform.setChunk(std::move(chunk1));
    partition_transform.work();
    partition_transform.setChunk(std::move(chunk2));
    partition_transform.setReceiveAllData(true);
    partition_transform.work();
    partition_transform.printOutputChunk();
}

}
