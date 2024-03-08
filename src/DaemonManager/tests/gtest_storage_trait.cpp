#include <Common/tests/gtest_global_context.h>
#include <DaemonManager/DaemonJobServerBGThread.h>
#include <Interpreters/Context.h>
#include <string>
#include <gtest/gtest.h>

using namespace DB::DaemonManager;
using namespace DB;

namespace GtestStorageTrait
{

TEST(StorageTraitTest, operations_test)
{
    {
        StorageTrait s{StorageTrait::Param {
                .is_cnch_merge_tree = true,
                .is_cnch_kafka = false,
                .is_cnch_unique = false,
                .is_cnch_refresh_materialized_view = false
            }};
        EXPECT_TRUE(s.isCnchMergeTree());
        EXPECT_FALSE(s.isCnchKafka());
        EXPECT_FALSE(s.isCnchUniqueAndNeedDedup());
        EXPECT_FALSE(s.isCnchRefreshMaterializedView());
    }
    {
        StorageTrait s{StorageTrait::Param {
                .is_cnch_merge_tree = false,
                .is_cnch_kafka = true,
                .is_cnch_unique = false,
                .is_cnch_refresh_materialized_view = false
            }};
        EXPECT_FALSE(s.isCnchMergeTree());
        EXPECT_TRUE(s.isCnchKafka());
        EXPECT_FALSE(s.isCnchUniqueAndNeedDedup());
        EXPECT_FALSE(s.isCnchRefreshMaterializedView());
    }
    {
        StorageTrait s{StorageTrait::Param {
                .is_cnch_merge_tree = false,
                .is_cnch_kafka = false,
                .is_cnch_unique = true,
                .is_cnch_refresh_materialized_view = false
            }};
        EXPECT_FALSE(s.isCnchMergeTree());
        EXPECT_FALSE(s.isCnchKafka());
        EXPECT_TRUE(s.isCnchUniqueAndNeedDedup());
        EXPECT_FALSE(s.isCnchRefreshMaterializedView());
    }
    {
        StorageTrait s{StorageTrait::Param {
                .is_cnch_merge_tree = false,
                .is_cnch_kafka = false,
                .is_cnch_unique = false,
                .is_cnch_refresh_materialized_view = true
            }};
        EXPECT_FALSE(s.isCnchMergeTree());
        EXPECT_FALSE(s.isCnchKafka());
        EXPECT_FALSE(s.isCnchUniqueAndNeedDedup());
        EXPECT_TRUE(s.isCnchRefreshMaterializedView());
    }
    {
        StorageTrait s;
        EXPECT_FALSE(s.isCnchMergeTree());
        EXPECT_FALSE(s.isCnchKafka());
        EXPECT_FALSE(s.isCnchUniqueAndNeedDedup());
        EXPECT_FALSE(s.isCnchRefreshMaterializedView());
    }
}

TEST(StorageTraitTest, StorageTraitForHive)
{
    std::string s = R"#(CREATE TABLE test.customer_address
(
    `ca_address_sk` Nullable(Int64),
    `ca_address_id` Nullable(String),
    `ca_street_number` Nullable(String),
    `ca_street_name` Nullable(String),
    `ca_street_type` Nullable(String),
    `ca_suite_number` Nullable(String),
    `ca_city` Nullable(String),
    `ca_county` Nullable(String),
    `ca_state` Nullable(String),
    `ca_zip` Nullable(String),
    `ca_country` Nullable(String),
    `ca_gmt_offset` Nullable(Float32),
    `ca_location_type` Nullable(String)
)
ENGINE = CnchHive('HIVE_METASTORE', 'HIVE_DATABASE', 'customer_address')
SETTINGS region = 'S3_REGION', endpoint = 'S3_ENDPOINT', ak_id = 'S3_ACCESS_KEY', ak_secret = 'S3_SECRET_KEY'
;)#";

    StorageTrait storage_trait = constructStorageTrait(getContext().context, "test", "customer_address", s);
    StorageTrait expected_result{StorageTrait::Param {
            .is_cnch_merge_tree = false,
            .is_cnch_kafka = false,
            .is_cnch_unique = false,
            .is_cnch_refresh_materialized_view = false
        }};

    EXPECT_EQ(storage_trait, expected_result);
}

TEST(StorageTraitTest, isCnchMergeTreeOrKafkaTest)
{
    {
        String engine_name{"CnchAggregatingMergeTree"};
        EXPECT_TRUE(isCnchMergeTreeOrKafka(engine_name));
    }

    {
        String engine_name{"CnchVersionedCollapsingMergeTree"};
        EXPECT_TRUE(isCnchMergeTreeOrKafka(engine_name));
    }

    {
        String engine_name{"CnchKafka"};
        EXPECT_TRUE(isCnchMergeTreeOrKafka(engine_name));
    }

    {
        String engine_name{"CnchHive"};
        EXPECT_FALSE(isCnchMergeTreeOrKafka(engine_name));
    }

    {
        String engine_name{"CnchS3"};
        EXPECT_FALSE(isCnchMergeTreeOrKafka(engine_name));
    }

    {
        String engine_name{"AggregatingMergeTree"};
        EXPECT_FALSE(isCnchMergeTreeOrKafka(engine_name));
    }
}
} // end namespace

