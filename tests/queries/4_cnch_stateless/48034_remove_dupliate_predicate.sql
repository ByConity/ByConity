drop table if exists 48038_table;
create table 48038_table (
    id UInt64,
    str String
) Engine = CnchMergeTree() order by id;


set enable_optimizer=1;

EXPLAIN
SELECT *
FROM 48038_table
WHERE (str = 'a') AND (id = 1) AND (str = 'a') AND (str = 'a');

EXPLAIN
SELECT *
FROM 48038_table
WHERE (str = 'a') OR (id = 1) OR (str = 'a') OR (str = 'a');