DROP TABLE IF EXISTS test_detach_partition_where;

CREATE TABLE IF NOT EXISTS test_detach_partition_where
(
    `c1` String,
    `c2` Int64,
    `my_partition` Date
)
    ENGINE = CnchMergeTree
PARTITION BY (my_partition, c1)
ORDER BY c2
SETTINGS index_granularity = 8192;

insert into test_detach_partition_where values ('1', 1, '2023-05-25'), ('2', 2, '2023-05-25'), ('00', 3, '2023-05-25');

select * from test_detach_partition_where order by `c1`;

select '--';

alter table test_detach_partition_where detach partition where my_partition = '2023-05-25' and c1 != '00';

select * from test_detach_partition_where order by `c1`;

DROP TABLE IF EXISTS test_detach_partition_where;