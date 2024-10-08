SET enable_optimizer = 1;
SET enable_create_topn_filtering_for_aggregating = 1;

EXPLAIN SELECT n, count(*)
        FROM (
                 (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 2 = 0)
                 UNION ALL
                 (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 3 = 0)
                 UNION ALL
                 (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 5 = 0)
             )
        GROUP BY n
        ORDER BY n
                    LIMIT 10;

SELECT n, count(*)
FROM (
         (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 2 = 0)
         UNION ALL
         (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 3 = 0)
         UNION ALL
         (SELECT n FROM (SELECT number as n FROM system.numbers LIMIT 30) WHERE n % 5 = 0)
     )
GROUP BY n
ORDER BY n
LIMIT 10;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS polygon_tx_v87;
CREATE TABLE polygon_tx_v87
(
    block_number UInt64,
    gas UInt64,
    gas_price Float64
)
ENGINE = CnchMergeTree() ORDER BY block_number;

explain select
            block_number,
            sum(gas * gas_price) as gas_fee
        from polygon_tx_v87
        where block_number > 10000000 and block_number < 20000000
        group by block_number
        order by block_number
            limit 10;

DROP TABLE IF EXISTS polygon_tx_v87;
