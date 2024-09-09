DROP TABLE if exists 48030_test_create_table;
CREATE TABLE 48030_test_create_table
(
    id UInt64,
    `int_col_1` Nullable(UInt64),
    `str_col_2` LowCardinality(Nullable(String)),
    `float_col_1` LowCardinality(Nullable(Float64)),
    `enum_col_1` Nullable(Enum('a'=1, 'b'=2, 'c'=3, 'd'=4))
)
ENGINE = CnchMergeTree
PARTITION BY id
CLUSTER BY `id` INTO 10 BUCKETS
PRIMARY KEY (`id`)
ORDER BY (`id`)
SETTINGS  index_granularity = 8192;


set enable_optimizer=1;

select int_col_1,str_col_2,enum_col_1,sum(float_col_1) as d from 48030_test_create_table group by int_col_1,str_col_2,enum_col_1 with rollup order by int_col_1,str_col_2,enum_col_1,d;
select '---';
select id,str_col_2,enum_col_1,sum(float_col_1) as d from 48030_test_create_table group by id,str_col_2,enum_col_1 with rollup order by id,str_col_2,enum_col_1,d;

insert into 48030_test_create_table values(1, 1, 'a', 10.0, 'a');

select '---';
select int_col_1,str_col_2,enum_col_1,sum(float_col_1) as d from 48030_test_create_table group by int_col_1,str_col_2,enum_col_1 with rollup order by int_col_1,str_col_2,enum_col_1,d;
select '---';
select id,str_col_2,enum_col_1,sum(float_col_1) as d from 48030_test_create_table group by id,str_col_2,enum_col_1 with rollup order by id,str_col_2,enum_col_1,d;
