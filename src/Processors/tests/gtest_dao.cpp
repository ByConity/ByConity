#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>

#include <Processors/QueryPipeline.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <Processors/IProcessor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/SinkToOutputStream.h>
#include <Processors/NullSink.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>

#include <Common/tests/gtest_global_context.h>
#include <iostream>

using namespace DB;

namespace UnitTestax12
{


/** Does nothing. Used for debugging and benchmarks.
  */
class NullBlockOutputStream : public IBlockOutputStream
{
public:
    NullBlockOutputStream(const Block & header_) : header(header_) {}
    Block getHeader() const override { return header; }
    void write(const Block & b) override
    {
        std::cout << "row count: " << b.rows() << '\n';
    }

private:
    Block header;
};


TEST(TestSinkSource, SimpleTest)
{
    constexpr size_t row_count = 100;
    constexpr size_t column_count = 1;
    constexpr unsigned int val = 1;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "count")};
    Chunk source_chunk = UnitTest::createUInt8Chunk(row_count, column_count, val);
    SourcePtr source1 = std::make_shared<SourceFromSingleChunk>(header, std::move(source_chunk));
    SourcePtr source2 = std::make_shared<SourceFromSingleChunk>(header,
        UnitTest::createUInt8Chunk(row_count, column_count, 2));

    auto aggregating_sorted_transform = std::make_shared<AggregatingSortedTransform>(header, 2, SortDescription{}, 100000);

    auto distinct_transform = std::make_shared<DistinctTransform>(header, SizeLimits{}, 0, Names{"count"});
    BlockOutputStreamPtr output_stream = std::make_shared<NullBlockOutputStream>(header);
    std::shared_ptr<ISink> empty_sink = std::make_shared<SinkToOutputStream>(output_stream);
    connect(source1->getPort(), aggregating_sorted_transform->getInputs().front());
    connect(source2->getPort(), aggregating_sorted_transform->getInputs().back());
    connect(aggregating_sorted_transform->getOutputPort(), distinct_transform->getInputPort());
    connect(distinct_transform->getOutputPort(), empty_sink->getPort());

    Processors processors;
    processors.emplace_back(std::move(source1));
    processors.emplace_back(std::move(source2));
    processors.emplace_back(std::move(aggregating_sorted_transform));
    processors.emplace_back(std::move(distinct_transform));
    processors.emplace_back(std::move(empty_sink));
    PipelineExecutor executor(processors);
    executor.execute(1);
    std::cout << "finish\n";
}
}

