SET optimize_on_insert = 0;

DROP TABLE IF EXISTS ha_merge_tree;
DROP TABLE IF EXISTS ha_collapsing_merge_tree;
DROP TABLE IF EXISTS ha_versioned_collapsing_merge_tree;
DROP TABLE IF EXISTS ha_summing_merge_tree;
DROP TABLE IF EXISTS ha_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS ha_aggregating_merge_tree;

DROP TABLE IF EXISTS ha_merge_tree_with_sampling;
DROP TABLE IF EXISTS ha_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS ha_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE IF EXISTS ha_summing_merge_tree_with_sampling;
DROP TABLE IF EXISTS ha_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE IF EXISTS ha_aggregating_merge_tree_with_sampling;


CREATE TABLE ha_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaMergeTree('/clickhouse/tables/test_10001/01/ha_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE ha_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaCollapsingMergeTree('/clickhouse/tables/test_10001/01/ha_collapsing_merge_tree/', 'r1', d, (a, b), 111, y);
CREATE TABLE ha_versioned_collapsing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaVersionedCollapsingMergeTree('/clickhouse/tables/test_10001/01/ha_versioned_collapsing_merge_tree/', 'r1', d, (a, b), 111, y, b);
CREATE TABLE ha_summing_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaSummingMergeTree('/clickhouse/tables/test_10001/01/ha_summing_merge_tree/', 'r1', d, (a, b), 111);
CREATE TABLE ha_summing_merge_tree_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaSummingMergeTree('/clickhouse/tables/test_10001/01/ha_summing_merge_tree_with_list_of_columns_to_sum/', 'r1', d, (a, b), 111, (y, z));
CREATE TABLE ha_aggregating_merge_tree
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaAggregatingMergeTree('/clickhouse/tables/test_10001/01/ha_aggregating_merge_tree/', 'r1', d, (a, b), 111);

CREATE TABLE ha_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaMergeTree('/clickhouse/tables/test_10001/01/ha_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE ha_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaCollapsingMergeTree('/clickhouse/tables/test_10001/01/ha_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, y);
CREATE TABLE ha_versioned_collapsing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaVersionedCollapsingMergeTree('/clickhouse/tables/test_10001/01/ha_versioned_collapsing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b, b), 111, y, b);
CREATE TABLE ha_summing_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaSummingMergeTree('/clickhouse/tables/test_10001/01/ha_summing_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);
CREATE TABLE ha_summing_merge_tree_with_sampling_with_list_of_columns_to_sum
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaSummingMergeTree('/clickhouse/tables/test_10001/01/ha_summing_merge_tree_with_sampling_with_list_of_columns_to_sum/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111, (y, z));
CREATE TABLE ha_aggregating_merge_tree_with_sampling
	(d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = HaAggregatingMergeTree('/clickhouse/tables/test_10001/01/ha_aggregating_merge_tree_with_sampling/', 'r1', d, sipHash64(a) + b, (a, sipHash64(a) + b), 111);


INSERT INTO ha_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_versioned_collapsing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_summing_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_summing_merge_tree_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_aggregating_merge_tree VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

INSERT INTO ha_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_versioned_collapsing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_summing_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_summing_merge_tree_with_sampling_with_list_of_columns_to_sum VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);
INSERT INTO ha_aggregating_merge_tree_with_sampling VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);


DROP TABLE ha_merge_tree;
DROP TABLE ha_collapsing_merge_tree;
DROP TABLE ha_versioned_collapsing_merge_tree;
DROP TABLE ha_summing_merge_tree;
DROP TABLE ha_summing_merge_tree_with_list_of_columns_to_sum;
DROP TABLE ha_aggregating_merge_tree;

DROP TABLE ha_merge_tree_with_sampling;
DROP TABLE ha_collapsing_merge_tree_with_sampling;
DROP TABLE ha_versioned_collapsing_merge_tree_with_sampling;
DROP TABLE ha_summing_merge_tree_with_sampling;
DROP TABLE ha_summing_merge_tree_with_sampling_with_list_of_columns_to_sum;
DROP TABLE ha_aggregating_merge_tree_with_sampling;
