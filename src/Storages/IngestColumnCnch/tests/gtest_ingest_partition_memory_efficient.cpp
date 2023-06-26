#include <Storages/IngestColumnCnch/memoryEfficientIngestColumnHelper.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <string>
#include <gtest/gtest.h>

using namespace DB;
using namespace DB::IngestColumn;
namespace
{


Block getSourceBlock1()
{
    Block input;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(1);
        col->insert(2);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s1");
        col->insert("s2");
        column2.column = std::move(col);
        input.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s1");
        col->insert("s2");
        column3.column = std::move(col);
        input.insert(column3);
    }
    return input;
}

Block getSourceBlock2()
{
    Block input;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(3);
        col->insert(4);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s3");
        col->insert("s4");
        column2.column = std::move(col);
        input.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s3");
        col->insert("s4");
        column3.column = std::move(col);
        input.insert(column3);
    }
    return input;
}

Block getSourceBlock3()
{
    Block input;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(6);
        col->insert(7);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s6");
        col->insert("s7");
        column2.column = std::move(col);
        input.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s6");
        col->insert("s7");
        column3.column = std::move(col);
        input.insert(column3);
    }
    return input;
}

Block getTargetBlock1()
{
    Block input;
    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(1);
        col->insert(3);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s1");
        col->insert("s3");
        column2.column = std::move(col);
        input.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("d1");
        col->insert("d3");
        column3.column = std::move(col);
        input.insert(column3);
    }
    return input;
}

Block getTargetBlock2()
{
    Block input;
    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(5);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s5");
        column2.column = std::move(col);
        input.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("d5");
        column3.column = std::move(col);
        input.insert(column3);
    }
    return input;
}

size_t countNotMatch(const HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & input)
{
    size_t not_match_count = 0;
    for (const auto & p : input)
    {
        if (p.getMapped().getExistStatus() == 0)
            ++not_match_count;
    }
    return not_match_count;
}

size_t countMatch(const HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & input)
{
    size_t match_count = 0;
    for (const auto & p : input)
    {
        if (p.getMapped().getExistStatus() == 1u)
            ++match_count;
    }
    return match_count;
}

TEST(ingest_partition_memory_efficient, keyInfoTest)
{
    {
        KeyInfo info;
        EXPECT_EQ(info.getPartID(), 0);
        EXPECT_EQ(info.getExistStatus(), 0);
        info.updateExistStatus(0);
        EXPECT_EQ(info.getPartID(), 0);
        EXPECT_EQ(info.getExistStatus(), 0);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), 0);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(0);
        EXPECT_EQ(info.getPartID(), 0);
        EXPECT_EQ(info.getExistStatus(), 0);
    }

    {
        KeyInfo info{19, 0};
        EXPECT_EQ(info.getPartID(), 19);
        EXPECT_EQ(info.getExistStatus(), 0);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), 19);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), 19);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(0);
        EXPECT_EQ(info.getPartID(), 19);
        EXPECT_EQ(info.getExistStatus(), 0);
    }

    {
        KeyInfo info{10, 0};
        EXPECT_EQ(info.getPartID(), 10);
        EXPECT_EQ(info.getExistStatus(), 0);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), 10);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), 10);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(0);
        EXPECT_EQ(info.getPartID(), 10);
        EXPECT_EQ(info.getExistStatus(), 0);
    }

    {
        constexpr size_t max_number_of_parts = ((std::numeric_limits<uint32_t>::max() -1)/ 2);
        KeyInfo info{max_number_of_parts, 0};
        EXPECT_EQ(info.getPartID(), max_number_of_parts);
        EXPECT_EQ(info.getExistStatus(), 0);
        info.updateExistStatus(1);
        EXPECT_EQ(info.getPartID(), max_number_of_parts);
        EXPECT_EQ(info.getExistStatus(), 1);
        info.updateExistStatus(0);
        EXPECT_EQ(info.getPartID(), max_number_of_parts);
        EXPECT_EQ(info.getExistStatus(), 0);
    }

}

TEST(ingest_partition_memory_efficient, combineHashmapTest)
{
    std::vector<std::string> strs {
        "hello",
        "world",
        "hi",
        "worlds"
    };

    std::vector<HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash>> hashmap_per_threads(2);
    hashmap_per_threads[0].insert({StringRef{strs[0]}, KeyInfo{1,1}});
    hashmap_per_threads[0].insert({StringRef{strs[1]}, KeyInfo{2,1}});
    hashmap_per_threads[1].insert({StringRef{strs[2]}, KeyInfo{3,0}});
    hashmap_per_threads[1].insert({StringRef{strs[3]}, KeyInfo{4,0}});

    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> res = DB::IngestColumn::combineHashmaps(std::move(hashmap_per_threads));
    EXPECT_EQ(res.size(), 4);

    {
        auto it = res.find(strs[0]);
        EXPECT_NE(it, res.end());
        KeyInfo expected_res{1,1};
        EXPECT_EQ(it->getMapped(), expected_res);
    }

    {
        auto it = res.find(strs[1]);
        EXPECT_NE(it, res.end());
        KeyInfo expected_res{2,1};
        EXPECT_EQ(it->getMapped(), expected_res);
    }

    {
        auto it = res.find(strs[2]);
        EXPECT_NE(it, res.end());
        KeyInfo expected_res{3,0};
        EXPECT_EQ(it->getMapped(), expected_res);
    }

    {
        auto it = res.find(strs[3]);
        EXPECT_NE(it, res.end());
        KeyInfo expected_res{4,0};
        EXPECT_EQ(it->getMapped(), expected_res);
    }
}

const Strings ordered_key_names{"i", "s"};
const Strings ingest_column_names{"d"};

TEST(ingest_partition_memory_efficient, buildHashTableFromBlockTestNormalCase)
{
    using DB::IngestColumn::buildHashTableFromBlock;
    Arena keys_pool;
    size_t bucket_num = 0;
    constexpr UInt32 part_id = 19;

    Block input = getSourceBlock1();

    {
        size_t number_of_bucket = 1;
        HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> res;
        buildHashTableFromBlock(part_id, input, ordered_key_names, res, bucket_num, keys_pool, number_of_bucket);
        EXPECT_EQ(res.size(), 2);
    }

    {
        size_t number_of_bucket = 2;
        HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> shard1_res;
        HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> shard2_res;
        buildHashTableFromBlock(part_id, input, ordered_key_names, shard1_res, 0, keys_pool, number_of_bucket);
        buildHashTableFromBlock(part_id, input, ordered_key_names, shard2_res, 1, keys_pool, number_of_bucket);
        size_t total_size = shard1_res.size() + shard2_res.size();
        EXPECT_TRUE((shard1_res.size() < 2) || (shard2_res.size() < 2));
        EXPECT_EQ(total_size, 2);
    }
}


TEST(ingest_partition_memory_efficient, buildHashTableFromBlockTestDuplicateCase)
{
    using DB::IngestColumn::buildHashTableFromBlock;
    Arena keys_pool;
    size_t bucket_num = 0;
    constexpr UInt32 part_id = 19;

    Block input;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(1);
        col->insert(1);
        column1.column = std::move(col);
        input.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "d";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("hello");
        col->insert("hello");
        column2.column = std::move(col);
        input.insert(column2);
    }

    size_t number_of_bucket = 1;
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> res;
    auto build_lambda = [&] ()
    {
        buildHashTableFromBlock(part_id, input, ordered_key_names, res, bucket_num, keys_pool, number_of_bucket);
    };
    EXPECT_THROW(build_lambda(), Exception);
}

TEST(ingest_partition_memory_efficient, probeHashTableFromBlockTestNormalCase)
{
    using DB::IngestColumn::buildHashTableFromBlock;
    using DB::IngestColumn::probeHashTableFromBlock;
    Arena keys_pool;
    const size_t bucket_num = 0;
    constexpr UInt32 source_part_id1 = 18;
    constexpr UInt32 source_part_id2 = 19;
    constexpr UInt32 target_part_id1 = 20;
    constexpr UInt32 target_part_id2 = 21;
    std::unordered_map<UInt32, std::set<UInt32>> target_part_index;
    const size_t number_of_bucket = 1;
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> source_hash_map;
    buildHashTableFromBlock(source_part_id1, getSourceBlock1(), ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket);
    buildHashTableFromBlock(source_part_id2, getSourceBlock2(), ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket);
    EXPECT_EQ(source_hash_map.size(), 4);
    EXPECT_EQ(countNotMatch(source_hash_map), 4);

    probeHashTableFromBlock(target_part_id1, getTargetBlock1(), ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket, target_part_index);
    EXPECT_EQ(countMatch(source_hash_map), 2);
    EXPECT_EQ(countNotMatch(source_hash_map), 2);

    std::unordered_map<UInt32, std::set<UInt32>> expected_target_part_index {
        {target_part_id1, {source_part_id1, source_part_id2}}
    };
    EXPECT_EQ(target_part_index, expected_target_part_index);

    probeHashTableFromBlock(target_part_id2, getTargetBlock2(), ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket, target_part_index);
    EXPECT_EQ(countMatch(source_hash_map), 2);
    EXPECT_EQ(countNotMatch(source_hash_map), 2);
    EXPECT_EQ(target_part_index, expected_target_part_index);
}

TEST(ingest_partition_memory_efficient, probeHashTableFromBlockTestDuplicateCase)
{
    using DB::IngestColumn::buildHashTableFromBlock;
    using DB::IngestColumn::probeHashTableFromBlock;
    Arena keys_pool;
    constexpr size_t bucket_num = 0;
    constexpr size_t number_of_bucket = 1;
    constexpr UInt32 source_part_id1 = 18;
    constexpr UInt32 target_part_id1 = 20;
    std::unordered_map<UInt32, std::set<UInt32>> target_part_index;

    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> source_hash_map;
    buildHashTableFromBlock(source_part_id1, getSourceBlock1(), ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket);

    Block target_block;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr col = ColumnInt32::create();
        col->insert(1);
        col->insert(1);
        column1.column = std::move(col);
        target_block.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        MutableColumnPtr col = ColumnString::create();
        col->insert("s1");
        col->insert("s1");
        column2.column = std::move(col);
        target_block.insert(column2);
    }

    auto probe_lambda = [&] ()
    {
        probeHashTableFromBlock(target_part_id1, target_block, ordered_key_names, source_hash_map, bucket_num, keys_pool, number_of_bucket, target_part_index);
    };

    EXPECT_THROW(probe_lambda(), Exception);
}

TEST(ingest_partition_memory_efficient, updateTargetBlockWithSourceBlock1)
{
    std::vector<FieldVector> update_data(1, FieldVector(2));
    std::pair<Block, std::vector<FieldVector>> target_block_data =
        std::make_pair(getTargetBlock1(), update_data);

    {
        size_t current_source_row_idx = 2;
        size_t current_target_row_idx = 0;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock1(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 2);
        EXPECT_EQ(current_target_row_idx, 0);
    }

    {
        size_t current_source_row_idx = 0;
        size_t current_target_row_idx = 2;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock1(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 0);
        EXPECT_EQ(current_target_row_idx, 2);
    }

    {
        size_t current_source_row_idx = 0;
        size_t current_target_row_idx = 1;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock1(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 2);
        EXPECT_EQ(current_target_row_idx, 1);
    }

    {
        size_t current_source_row_idx = 0;
        size_t current_target_row_idx = 0;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock1(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 2);
        EXPECT_EQ(current_target_row_idx, 1);
        const FieldVector & ingest_col = target_block_data.second.at(0);
        EXPECT_EQ(ingest_col.at(0).get<String>(), "s1");
        EXPECT_EQ(ingest_col.at(1), Field{});
    }

    {
        size_t current_source_row_idx = 0;
        size_t current_target_row_idx = 0;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock2(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 1);
        EXPECT_EQ(current_target_row_idx, 2);
        const FieldVector & ingest_col = target_block_data.second.at(0);
        EXPECT_EQ(ingest_col.at(0).get<String>(), "s1");
        EXPECT_EQ(ingest_col.at(1).get<String>(), "s3");
    }
}

TEST(ingest_partition_memory_efficient, updateTargetBlockWithSourceBlock2)
{
    std::vector<FieldVector> update_data(1, FieldVector(1));
    std::pair<Block, std::vector<FieldVector>> target_block_data =
        std::make_pair(getTargetBlock2(), update_data);

    {
        size_t current_source_row_idx = 0;
        size_t current_target_row_idx = 0;

        std::tie(current_source_row_idx, current_target_row_idx) = IngestColumn::updateTargetBlockWithSourceBlock(
            target_block_data,
            getSourceBlock1(),
            current_source_row_idx,
            current_target_row_idx,
            ordered_key_names,
            ingest_column_names);

        EXPECT_EQ(current_source_row_idx, 2);
        EXPECT_EQ(current_target_row_idx, 0);
    }
}

TEST(ingest_partition_memory_efficient, updateWithSourceBlock)
{
    std::vector<std::pair<Block, std::vector<FieldVector>>> data;

    {
        std::vector<FieldVector> update_data(1, FieldVector(2));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock1(), update_data);
        data.push_back(target_block_data);
    }
    {
        std::vector<FieldVector> update_data(1, FieldVector(1));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock2(), update_data);
        data.push_back(target_block_data);
    }

    TargetPartData target_data{std::move(data), ordered_key_names, ingest_column_names, Block{}, std::vector<UInt8>{1}, false};
    EXPECT_FALSE(target_data.updateWithSourceBlock(getSourceBlock1()));
    EXPECT_EQ(target_data.getCurrentBlockIdx(), 0);
    EXPECT_EQ(target_data.getCurrentRowInBlockIdx(), 1);
    EXPECT_FALSE(target_data.updateWithSourceBlock(getSourceBlock2()));
    EXPECT_EQ(target_data.getCurrentBlockIdx(), 1);
    EXPECT_EQ(target_data.getCurrentRowInBlockIdx(), 0);
    EXPECT_TRUE(target_data.updateWithSourceBlock(getSourceBlock3()));
    EXPECT_EQ(target_data.getCurrentBlockIdx(), 2);
    EXPECT_EQ(target_data.getCurrentRowInBlockIdx(), 0);

    data = target_data.getData();
    const FieldVector & block1_values = data.at(0).second.at(0);
    EXPECT_NE(block1_values.at(0), Field{});
    EXPECT_EQ(block1_values.at(0).get<String>(), "s1");
    EXPECT_NE(block1_values.at(1), Field{});
    EXPECT_EQ(block1_values.at(1).get<String>(), "s3");
    const FieldVector & block2_values = data.at(1).second.at(0);
    EXPECT_EQ(block2_values.at(0), Field{});
}

TEST(ingest_partition_memory_efficient, updateTargetDataWithSourceData)
{
    std::vector<std::pair<Block, std::vector<FieldVector>>> data;

    {
        std::vector<FieldVector> update_data(1, FieldVector(2));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock1(), update_data);
        data.push_back(target_block_data);
    }
    {
        std::vector<FieldVector> update_data(1, FieldVector(1));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock2(), update_data);
        data.push_back(target_block_data);
    }

    TargetPartData target_data{std::move(data), ordered_key_names, ingest_column_names, Block{}, std::vector<UInt8>{1}, false};


    BlockInputStreamPtr source_stream1 = std::make_shared<BlocksListBlockInputStream>(BlocksList{getSourceBlock3()});
    updateTargetDataWithSourceData(*source_stream1, target_data);
    EXPECT_EQ(target_data.getCurrentBlockIdx(), 2);
    EXPECT_EQ(target_data.getCurrentRowInBlockIdx(), 0);
    {
        data = target_data.getData();
        const FieldVector & block1_values = data.at(0).second.at(0);
        EXPECT_EQ(block1_values.at(0), Field{});
        EXPECT_EQ(block1_values.at(1), Field{});
        const FieldVector & block2_values = data.at(1).second.at(0);
        EXPECT_EQ(block2_values.at(0), Field{});
    }

    BlockInputStreamPtr source_stream2 = std::make_shared<BlocksListBlockInputStream>(BlocksList{getSourceBlock1(),getSourceBlock2()});
    updateTargetDataWithSourceData(*source_stream2, target_data);
    EXPECT_EQ(target_data.getCurrentBlockIdx(), 1);
    EXPECT_EQ(target_data.getCurrentRowInBlockIdx(), 0);

    {
        data = target_data.getData();
        const FieldVector & block1_values = data.at(0).second.at(0);
        EXPECT_NE(block1_values.at(0), Field{});
        EXPECT_EQ(block1_values.at(0).get<String>(), "s1");
        EXPECT_NE(block1_values.at(1), Field{});
        EXPECT_EQ(block1_values.at(1).get<String>(), "s3");
        const FieldVector & block2_values = data.at(1).second.at(0);
        EXPECT_EQ(block2_values.at(0), Field{});
    }
}

Block getHeader()
{
    Block res;

    {
        ColumnWithTypeAndName column1;
        column1.name = "i";
        column1.type = std::make_shared<DataTypeInt32>();
        res.insert(column1);
    }

    {
        ColumnWithTypeAndName column2;
        column2.name = "s";
        column2.type = std::make_shared<DataTypeString>();
        res.insert(column2);
    }

    {
        ColumnWithTypeAndName column3;
        column3.name = "d";
        column3.type = std::make_shared<DataTypeString>();
        res.insert(column3);
    }

    return res;
}

TEST(ingest_partition_memory_efficient, makeBlockListFromUpdateData_not_ingest_default_column_value_if_not_provided)
{
    std::vector<std::pair<Block, std::vector<FieldVector>>> data;

    {
        std::vector<FieldVector> update_data(1, FieldVector(2));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock1(), update_data);
        data.push_back(target_block_data);
    }
    {
        std::vector<FieldVector> update_data(1, FieldVector(1));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock2(), update_data);
        data.push_back(target_block_data);
    }

    TargetPartData target_data{std::move(data), ordered_key_names, ingest_column_names, getHeader(), std::vector<UInt8>{1}, false};
    target_data.updateWithSourceBlock(getSourceBlock1());
    target_data.updateWithSourceBlock(getSourceBlock2());
    target_data.updateWithSourceBlock(getSourceBlock3());

    BlocksList block_list = makeBlockListFromUpdateData(target_data);
    EXPECT_EQ(block_list.size(), 2);
    Block block1 = block_list.front();
    EXPECT_EQ(block1.rows(), 2);
    {
        ColumnPtr col = block1.getColumns().at(0);
        EXPECT_EQ((*col)[0].get<String>(), "s1");
        EXPECT_EQ((*col)[1].get<String>(), "s3");
    }

    Block block2 = block_list.back();
    EXPECT_EQ(block2.rows(), 1);
    {
        ColumnPtr col = block2.getColumns().at(0);
        EXPECT_EQ((*col)[0].get<String>(), "d5");
    }
}

TEST(ingest_partition_memory_efficient, makeBlockListFromUpdateData_ingest_default_column_value_if_not_provided)
{
    std::vector<std::pair<Block, std::vector<FieldVector>>> data;

    {
        std::vector<FieldVector> update_data(1, FieldVector(2));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock1(), update_data);
        data.push_back(target_block_data);
    }
    {
        std::vector<FieldVector> update_data(1, FieldVector(1));
        std::pair<Block, std::vector<FieldVector>> target_block_data =
            std::make_pair(getTargetBlock2(), update_data);
        data.push_back(target_block_data);
    }

    TargetPartData target_data{std::move(data), ordered_key_names, ingest_column_names, getHeader(), std::vector<UInt8>{1}, true};
    target_data.updateWithSourceBlock(getSourceBlock1());
    target_data.updateWithSourceBlock(getSourceBlock2());
    target_data.updateWithSourceBlock(getSourceBlock3());

    BlocksList block_list = makeBlockListFromUpdateData(target_data);
    EXPECT_EQ(block_list.size(), 2);
    Block block1 = block_list.front();
    EXPECT_EQ(block1.rows(), 2);
    {
        ColumnPtr col = block1.getColumns().at(0);
        EXPECT_EQ((*col)[0].get<String>(), "s1");
        EXPECT_EQ((*col)[1].get<String>(), "s3");
    }

    Block block2 = block_list.back();
    EXPECT_EQ(block2.rows(), 1);
    {
        ColumnPtr col = block2.getColumns().at(0);
        EXPECT_EQ((*col)[0].get<String>(), "");
    }
}

} /// end anonymous namespace
