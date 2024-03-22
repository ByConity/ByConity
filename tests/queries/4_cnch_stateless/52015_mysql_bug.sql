set dialect_type = 'MYSQL';
drop database if exists test_char;
create database test_char;
CREATE TABLE test_char.table_map (
    `id` Int, 
    `array` Array(String),
    `map` Map(String, Int) not null
) ENGINE=CnchMergeTree 
order by id;
INSERT INTO test_char.table_map VALUES (1, ['98', '99'], {'key1':1, 'key2':10}), (2, ['101', '102'], {'key1':2,'key2':20}), (3, ['103', '104'], {'key1':3,'key2':30}),  (4, ['103', '104'], {'key1':99,'key2':130});
select char(map['key1']) from test_char.table_map where id =4;

select TIMESTAMP(20231221010203);
