drop table if exists ts_table_local;
drop table if exists ts_table;

CREATE TABLE ts_table_local (`p_date` Date, `hour` String, `server_time` Nullable(Int64)) ENGINE = MergeTree() PARTITION BY (p_date, hour) ORDER BY p_date;
CREATE TABLE ts_table as ts_table_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 'ts_table_local');

set enable_optimizer=1;
explain distributed WITH
    t AS
    (
        SELECT server_time
        FROM ts_table
    ),
    e AS
    (
        SELECT sum(server_time)
        FROM ts_table
        GROUP BY hour
    )
SELECT *
FROM
(
    SELECT server_time
    FROM ts_table
    UNION ALL
    SELECT *
    FROM e
    UNION ALL
    SELECT *
    FROM e
);
