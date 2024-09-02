#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchDataAdapter.h>
#include <Protos/DataModelHelpers.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "MergeTreeCommon/MergeTreeMetaBase.h"
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>

using namespace DB;


namespace TestMock
{
class CnchDataAdapterTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        tryRegisterStorages();
        tryRegisterDisks();
        getContext().resetStoragePolicy();
    }
};

DataModelPartPtr
createPart()
{
    DataModelPartPtr part_model = std::make_shared<Protos::DataModelPart>();
    Protos::DataModelPartInfo * info_model = part_model->mutable_part_info();

    info_model->set_partition_id("all");
    info_model->set_min_block(123);
    info_model->set_max_block(321);
    info_model->set_level(0);
    info_model->set_mutation(2);
    info_model->set_hint_mutation(2);

    part_model->set_rows_count(0);
    part_model->set_partition_minmax("xxxx");
    part_model->set_marks_count(0);

    return part_model;
}

Protos::DataModelDeleteBitmap createDeleteBitmap(String partition_id = "my-partition-id") {
    Protos::DataModelDeleteBitmap bitmap;
    bitmap.set_partition_id(partition_id);
    bitmap.set_part_min_block(123);
    bitmap.set_part_max_block(321);
    bitmap.set_reserved(111);
    bitmap.set_type(DB::Protos::DataModelDeleteBitmap_Type_Delta);
    bitmap.set_txn_id(9999);
    bitmap.set_commit_time(3123);
    return bitmap;
}


StoragePtr createTable(const String & create_query, ContextMutablePtr context)
{
    return createStorageFromQuery(create_query, context);
}

/// Tests for parts adapters.
TEST_F(CnchDataAdapterTest, PartsAdapter)
{
    auto part_model = createPart();

    String query = "create table gztest.test(id Int32) ENGINE=CnchMergeTree order by id";
    StoragePtr storage = createTable(query, getContext().context);
    const auto & merge_tree = dynamic_cast<const MergeTreeMetaBase &>(*storage);
    MutableMergeTreeDataPartCNCHPtr part = createPartFromModel(merge_tree, *part_model);

    {
        DataPartAdapter adapter(part);

        EXPECT_EQ(adapter.getName(), "all_123_321_0_2");
        EXPECT_EQ(adapter.getPartitionId(), "all");
    }

    {
        IMergeTreeDataPartAdapter adapter(part);
        EXPECT_EQ(adapter.getName(), "all_123_321_0_2");
        EXPECT_EQ(adapter.getPartitionId(), "all");
    }

    {
        ServerDataPartAdapter adapter(merge_tree, DB::Protos::DataModelPart(*part_model));
        EXPECT_EQ(adapter.getName(), "all_123_321_0_2");
        EXPECT_EQ(adapter.getPartitionId(), "all");
        EXPECT_EQ(adapter.getCommitTime(), 2);
    }

    {
        PartPlainTextAdapter adapter("all_123_321_0_2", MERGE_TREE_CHCH_DATA_STORAGTE_VERSION);
        EXPECT_EQ(adapter.getName(), "all_123_321_0_2");
        EXPECT_EQ(adapter.getPartitionId(), "all");
    }
}

TEST_F(CnchDataAdapterTest, DeleteBitmapsAdapter)
{

    String query = "create table gztest.test(id Int32) ENGINE=CnchMergeTree order by id";
    StoragePtr storage = createTable(query, getContext().context);
    const auto & merge_tree = dynamic_cast<const MergeTreeMetaBase &>(*storage);
    auto bitmap = createDeleteBitmap();

    auto data = std::make_shared<DeleteBitmapMeta>(merge_tree, std::make_shared<decltype(bitmap)>(bitmap));

    {
        DeleteBitmapAdapter adapter(data);
        EXPECT_EQ(adapter.getName(), "my-partition-id_123_321_111_1_9999");
        EXPECT_EQ(adapter.getPartitionId(), "my-partition-id");
        EXPECT_EQ(adapter.getCommitTime(), 3123);
        EXPECT_EQ(adapter.getData(), data->getModel());
        EXPECT_EQ(adapter.toData(), data);
    }

    {
        DeleteBitmapAdapter adapter(merge_tree, data->getModel());
        EXPECT_EQ(adapter.getName(), "my-partition-id_123_321_111_1_9999");
        EXPECT_EQ(adapter.getPartitionId(), "my-partition-id");
        EXPECT_EQ(adapter.getCommitTime(), 3123);
        EXPECT_EQ(adapter.getData(), data->getModel());
        EXPECT_TRUE(adapter.toData()->sameBlock(*data));
    }

    {
        DeleteBitmapPlainTextAdapter adapter(dataModelName(bitmap));

        EXPECT_EQ(adapter.getName(), "my-partition-id_123_321_111_1_9999");
        EXPECT_EQ(adapter.getPartitionId(), "my-partition-id");
    }
}
}
