#include <CloudServices/CnchCreateQueryHelper.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <gtest/gtest.h>

using namespace DB;

class StorageCNCHTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        tryRegisterStorages();
        tryRegisterDisks();
        tryRegisterFunctions();
        getContext().resetStoragePolicy();
    }
};

TEST_F(StorageCNCHTest, TableDefinitionHash)
{
    String create_table_query = 
        "create table test.t (app_id Int64, event_date Date, hash_uid String, event String, time DateTime) Engine = CnchMergeTree PARTITION BY (app_id, event_date) CLUSTER BY cityHash64(hash_uid) INTO 1000 BUCKETS SPLIT_NUMBER 64000 ORDER BY (event, hash_uid, time)";

    auto storage = createStorageFromQuery(create_table_query, getContext().context);

    UInt64 determin_hash = 15422756269316878347UL;
    UInt64 clang_hash = 17480696564937362659UL;

    auto table_definition_hash = storage->getTableHashForClusterBy();

    ASSERT_EQ(table_definition_hash.getDeterminHash(), determin_hash);
    ASSERT_TRUE(table_definition_hash.match(determin_hash));

    ASSERT_TRUE(table_definition_hash.match(clang_hash));

#if __SIZEOF_SIZE_T__ == 8
    UInt64 gcc_hash = 9877663151203055078UL;
    ASSERT_TRUE(table_definition_hash.match(gcc_hash));

    ASSERT_EQ(table_definition_hash.toString(), "determin_hash: 15422756269316878347, v1_hash: 9877663151203055078, v2_hash: 17480696564937362659, v1_quoted_hash: 15528011564892461728");
#endif
}

TEST_F(StorageCNCHTest, TableDefinitionHashQuote)
{
    String create_table_query = 
        "CREATE TABLE `32664146980.data_warehouse`.spam_collections_with_bucket ( \
    `collection_id` String, \
    `spam_score` UInt64, \
    `name` Nullable(String)) \
    ENGINE = CnchMergeTree \
    CLUSTER BY collection_id INTO 16 BUCKETS SPLIT_NUMBER 1920 WITH_RANGE \
    ORDER BY collection_id \
    UNIQUE KEY collection_id";

    auto storage = createStorageFromQuery(create_table_query, getContext().context);

    UInt64 determin_hash = 184170134261831116UL;
    UInt64 v2_hash = 729677759832070748UL;

    auto table_definition_hash = storage->getTableHashForClusterBy();

    ASSERT_EQ(table_definition_hash.getDeterminHash(), determin_hash);
    ASSERT_TRUE(table_definition_hash.match(determin_hash));

    ASSERT_TRUE(table_definition_hash.match(v2_hash));

#if __SIZEOF_SIZE_T__ == 8
    UInt64 v1_hash = 5314492121817770126UL;
    ASSERT_TRUE(table_definition_hash.match(v1_hash));

    UInt64 v1_quoted_hash = 11389215825359256540UL;
    ASSERT_TRUE(table_definition_hash.match(v1_quoted_hash));

    ASSERT_EQ(table_definition_hash.toString(), "determin_hash: 184170134261831116, v1_hash: 5314492121817770126, v2_hash: 729677759832070748, v1_quoted_hash: 11389215825359256540");
#endif
}
