#include <Common/tests/gtest_global_context.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <gtest/gtest.h>

using namespace DB;

namespace GtestCreateQueryForCloudTable
{

const UUID g_uuid1 = UUID{UInt128{0, 1}};
const StorageID g_storage_id1{"db1", "tb1", g_uuid1};

TEST(test_create_query_for_cloud_table, unique_with_version)
{
    String query{"CREATE TABLE db1.tb1 (`event_time` DateTime, `id` UInt64, `s` String, `m1` UInt32, `m2` UInt64, `seq` UInt32) Engine = CnchMergeTree(event_time) PARTITION BY toDate(event_time) PRIMARY KEY s ORDER BY (s, id) UNIQUE KEY id"};

    CnchStorageCommonHelper helper{g_storage_id1, "db1", "tb1"};
    String res = helper.getCreateQueryForCloudTable(query, "tb1_44123124122", getContext().context);
    String expected = R"#(CREATE TABLE db1.tb1_44123124122
(
    `event_time` DateTime,
    `id` UInt64,
    `s` String,
    `m1` UInt32,
    `m2` UInt64,
    `seq` UInt32
)
ENGINE = CloudMergeTree(db1, tb1, event_time)
PARTITION BY toDate(event_time)
PRIMARY KEY s
ORDER BY (s, id)
UNIQUE KEY id
)#";
    EXPECT_EQ(res, expected);
}

TEST(test_create_query_for_cloud_table, unique_without_version)
{
    String query{"CREATE TABLE db1.tb1 (`event_time` DateTime, `id` UInt64, `s` String, `m1` UInt32, `m2` UInt64, `seq` UInt32) Engine = CnchMergeTree() PARTITION BY toDate(event_time) PRIMARY KEY s ORDER BY (s, id) UNIQUE KEY id"};

    CnchStorageCommonHelper helper{g_storage_id1, "db1", "tb1"};
    String res = helper.getCreateQueryForCloudTable(query, "tb1_44123124122", getContext().context);
    String expected = R"#(CREATE TABLE db1.tb1_44123124122
(
    `event_time` DateTime,
    `id` UInt64,
    `s` String,
    `m1` UInt32,
    `m2` UInt64,
    `seq` UInt32
)
ENGINE = CloudMergeTree(db1, tb1)
PARTITION BY toDate(event_time)
PRIMARY KEY s
ORDER BY (s, id)
UNIQUE KEY id
)#";
    EXPECT_EQ(res, expected);
}

TEST(test_create_query_for_cloud_table, version_collapse)
{
    String query{"CREATE TABLE db1.tb1 ( UserID UInt64, PageViews UInt8, Duration UInt8, Sign Int8, Version UInt8) Engine=CnchVersionedCollapsingMergeTree(Sign, Version) ORDER BY UserID"};

    CnchStorageCommonHelper helper{g_storage_id1, "db1", "tb1"};
    String res = helper.getCreateQueryForCloudTable(query, "tb1_44123124122", getContext().context);
    String expected = R"#(CREATE TABLE db1.tb1_44123124122
(
    `UserID` UInt64,
    `PageViews` UInt8,
    `Duration` UInt8,
    `Sign` Int8,
    `Version` UInt8
)
ENGINE = CloudVersionedCollapsingMergeTree(db1, tb1, Sign, Version)
ORDER BY UserID
)#";
    EXPECT_EQ(res, expected);
}

TEST(test_create_query_for_cloud_table, s3)
{
    String query{"CREATE TABLE db1.tb1 (`id` String, `age` String) ENGINE = CnchS3('http://some_link/some_path/some_file.csv', 'CSV', 'none', 'AKkkkkkkkkk', 'sKkkkkkkkkkkkkkkkkkkk')"};
    Strings engine_args{"http://some_link/some_path/some_file.csv", "CSV", "none", "AKkkkkkkkkk", "sKkkkkkkkkkkkkkkkkkkk"};
    CnchStorageCommonHelper helper{g_storage_id1, "db1", "tb1"};
    String res = helper.getCreateQueryForCloudTable(query, "tb1_44123124122", getContext().context, false, std::nullopt, engine_args);

    String expected = R"#(CREATE TABLE db1.tb1_44123124122
(
    `id` String,
    `age` String
)
ENGINE = CloudS3(db1, tb1, `http://some_link/some_path/some_file.csv`, CSV, none, AKkkkkkkkkk, sKkkkkkkkkkkkkkkkkkkk)
)#";
    EXPECT_EQ(res, expected);
}

TEST(test_create_query_for_cloud_table, with_local_db)
{
    String query{"CREATE TABLE db1.tb1 (`event_time` DateTime, `id` UInt64, `s` String, `m1` UInt32, `m2` UInt64, `seq` UInt32) Engine = CnchMergeTree() PARTITION BY toDate(event_time) PRIMARY KEY s ORDER BY (s, id)"};

    CnchStorageCommonHelper helper{g_storage_id1, "db1", "tb1"};
    String res = helper.getCreateQueryForCloudTable(query, "tb1_44123124122", nullptr, false, std::nullopt, Strings{}, "db1_44123124122");
    String expected = R"#(CREATE TABLE db1_44123124122.tb1_44123124122
(
    `event_time` DateTime,
    `id` UInt64,
    `s` String,
    `m1` UInt32,
    `m2` UInt64,
    `seq` UInt32
)
ENGINE = CloudMergeTree(db1, tb1)
PARTITION BY toDate(event_time)
PRIMARY KEY s
ORDER BY (s, id)
)#";
    EXPECT_EQ(res, expected);
}

}
