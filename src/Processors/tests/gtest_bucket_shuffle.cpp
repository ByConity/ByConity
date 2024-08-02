#include <cstddef>
#include <memory>
#include <string>
#include <gtest/gtest.h>
#include <common/types.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <MergeTreeCommon/CnchBucketTableCommon.h>


using namespace DB;


Block generateBlockWithTwoColumns(size_t total_rows)
{
    auto col_uint64 = ColumnUInt64::create(total_rows, 0);
    auto & col_uint64_data = col_uint64->getData();
    auto col_string = ColumnString::create();
    for (size_t i = 0; i < total_rows; i++)
    {
        col_uint64_data[i] = i;
        String str = "bucket_" + std::to_string(i) ;
        col_string->insertData(str.data(), str.size());
    }
    ColumnWithTypeAndName column_1{std::move(col_uint64), std::make_shared<DataTypeUInt64>(), "column_1"};
    ColumnWithTypeAndName column_2{std::move(col_string), std::make_shared<DataTypeString>(), "column_2"};


    ColumnsWithTypeAndName columns;
    columns.emplace_back(std::move(column_1));
    columns.emplace_back(std::move(column_2));
    return Block(columns);
}


bool comparePrepareBucketColumnWithBucketFunction(Block & expected, ColumnPtr result)
{
    auto expected_col = expected.getByName(COLUMN_BUCKET_NUMBER).column;

    if (expected_col->size() != result->size())
        return false;
    for(size_t i = 0; i < expected_col->size(); i++)
    {
        if(expected_col->getUInt(i) != result->getUInt(i))
            return false;
    }
    return true;
}

ColumnPtr executeBucketFunction(Block & block, const Names & bucket_columns, const Int64 & split_number, const bool is_with_range, const Int64 total_shard_num, ContextPtr context)
{
    String func_name = "sipHashBuitin";
    if(split_number  && bucket_columns.size() == 1)
        func_name = "dtspartition";
    Array params;
    params.emplace_back(Field(func_name));
    params.emplace_back(Field(static_cast<DB::UInt64>(total_shard_num)));
    params.emplace_back(Field(is_with_range));
    params.emplace_back(Field(static_cast<DB::UInt64>(split_number)));


    ColumnsWithTypeAndName arguments;
    for (const auto & name: bucket_columns)
    {
        arguments.push_back(block.getByName(name));
    }

    auto func = RepartitionTransform::getRepartitionHashFunction("bucket", arguments, context, params);
    return func->execute(arguments, RepartitionTransform::REPARTITION_FUNC_RESULT_TYPE, block.rows(), false);
}


bool executeAndComparePrepareBucketColumnWithBucketFunction(
    Block & block,
    Names bucket_columns,
    const Int64 & split_number,
    const bool is_with_range,
    const Int64 total_shard_num,
    ContextPtr context)
{
    auto expected = block;
    prepareBucketColumn(expected, bucket_columns, split_number, is_with_range, total_shard_num, context, false);
    auto result = executeBucketFunction(block, bucket_columns, split_number, is_with_range, total_shard_num, context);
    return comparePrepareBucketColumnWithBucketFunction(expected, result);
}

TEST(BucketShuffleTest, BucketFunctionTest)
{
    tryRegisterFunctions();
    auto block = generateBlockWithTwoColumns(5);
    auto local_context = Context::createCopy(getContext().context);


    // sipHashBuitin
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1"}, 0, false, 33, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_2"}, 0, false, 100, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1", "column_2"}, 300, false, 100, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1", "column_2"}, 50, true, 100, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1", "column_2"}, 50, true, 300, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1", "column_2"}, 50, true, 1, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1", "column_2"}, 1, true, 50, local_context));


    // dtspartition
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1"}, 300, false, 100, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_1"}, 400, true, 99, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_2"}, 400, true, 99, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_2"}, 99, true, 101, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_2"}, 99, true, 1, local_context));
    ASSERT_TRUE(executeAndComparePrepareBucketColumnWithBucketFunction(block, {"column_2"}, 1, true, 102, local_context));

    // fail test
    auto expected = block;
    prepareBucketColumn(expected, {"column_1"}, 0, false, 33, local_context, false);
    auto result = executeBucketFunction(block, {"column_1"}, 0, false, 37, local_context);
    ASSERT_FALSE(comparePrepareBucketColumnWithBucketFunction(expected, result));
}


