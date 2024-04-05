#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>

#include <memory>
#include <string_view>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}


TEST(ParserDao, simple_test)
{
    String create_query = R"#(CREATE TABLE test_gipxk2.people_448825934011957253 UUID '46b8dd72-92b4-43af-81c8-63fd655d346c'
(
    `id` Int32,
    `name` String,
    `company_id` Int32,
    `state_id` Int32,
    `city_id` Int32
)
ENGINE = CloudMergeTree(test_gipxk2, people)
PARTITION BY id
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192, storage_policy = 'cnch_default_hdfs', cnch_temporary_table = 1
SETTINGS load_balancing = 'random', distributed_aggregation_memory_efficient = 0, log_queries = 1, distributed_product_mode = 'global', join_use_nulls = 0, max_execution_time = 180, log_comment = '40007_outer_join_to_inner_join.sql', send_logs_level = 'warning', data_type_default_nullable = 0, slow_query_ms = 0, max_rows_to_schedule_merge = 500000000, total_rows_to_schedule_merge = 0, strict_rows_to_schedule_merge = 50000000, enable_merge_scheduler = 0, cnch_max_cached_storage = 50000, enable_optimizer = 1, enable_optimizer_fallback = 0, exchange_timeout_ms = 300000, bsp_mode = 1)#";

    const char * begin = create_query.data();
    const char * end = create_query.data() + create_query.size();
    const std::string & description = "CreateCloudTable";
    ParserQueryWithOutput parser{end};
    ASTPtr ast = parseQuery(parser, begin, end, "CreateCloudTable", 0, 0);
}

