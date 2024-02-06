SET group_by_two_level_threshold_bytes = 1;
SET group_by_two_level_threshold = 1;

-- optimzier settings
SET enable_optimize_aggregate_memory_efficient=1;

DROP TABLE IF EXISTS dictinct_two_level;
CREATE TABLE dictinct_two_level (
    time DateTime64(3),
    domain String,
    subdomain String
) ENGINE = MergeTree ORDER BY time;

CREATE TABLE dictinct_two_level_distirubted (
    time DateTime64(3),
    domain String,
    subdomain String
) ENGINE =  Distributed(test_shard_localhost, currentDatabase(), 'dictinct_two_level');

INSERT INTO dictinct_two_level_distirubted SELECT 1546300800000, 'test.com', concat('foo', toString(number % 10000)) from numbers(10000);
INSERT INTO dictinct_two_level_distirubted SELECT 1546300800000, concat('test.com', toString(number / 10000)) , concat('foo', toString(number % 10000)) from numbers(10000);

SELECT
    domain, groupArraySample(5, 11111)(DISTINCT subdomain) AS example_subdomains
FROM dictinct_two_level_distirubted
GROUP BY domain ORDER BY domain, example_subdomains
LIMIT 10;

DROP TABLE IF EXISTS dictinct_two_level;
