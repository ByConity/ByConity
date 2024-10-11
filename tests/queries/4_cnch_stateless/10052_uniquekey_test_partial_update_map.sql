set enable_unique_partial_update = 1;

drop table if exists uniquekey_partial_update1;

------- test constrains of partial update features
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, partition_level_unique_keys = 0;  -- { serverError 344 }

------- test normal insert operation
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 0;

insert into uniquekey_partial_update1 values
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});

insert into uniquekey_partial_update1 (id, b, e) values (1, 'x', {'h': 12}), (1, '', {'c': 5}), (2, '7', {'f': 10}), (2, '8', {'g': 14});
select 'test normal insert operation, select1';
select * from uniquekey_partial_update1 order by id;

insert into uniquekey_partial_update1 (id, a, d) values (1, 4, [10, 20]), (2, 6, [50, 60]);
select 'test normal insert operation, select2';
select * from uniquekey_partial_update1 order by id;

drop table if exists uniquekey_partial_update1;

------- test partial update insert operation, enable merge_map_when_partial_update
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 1;

SET enable_unique_partial_update = 0;
insert into uniquekey_partial_update1 values
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});

insert into uniquekey_partial_update1 (id, b, e) values (1, 'x', {'h': 12}), (1, '', {'c': 5}), (2, '7', {'f': 10}), (2, '8', {'g': 14});
select 'test partial update insert operation, select1';
select * from uniquekey_partial_update1 order by id;

SET enable_unique_partial_update = 1;
insert into uniquekey_partial_update1 (id, a, d, _update_columns_) values (1, 4, [10, 20], 'id,d'), (2, 6, [50, 60], 'id,a');
select 'test partial update insert operation, select2';
select * from uniquekey_partial_update1 order by id;

------- test partial update insert select operation, enable merge_map_when_partial_update
drop table if exists uniquekey_partial_update_source_table;
drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;

create table uniquekey_partial_update_source_table (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id;
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 1;

insert into uniquekey_partial_update_source_table (id, a, b, c, d, e) values (1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});
select 'test partial update insert select operation, select1, from source table';
select * from uniquekey_partial_update_source_table order by id, a, b, c;

insert into uniquekey_partial_update1 (id, a, b, c, d, e, _update_columns_) select id, a, b, c, d, e, 'id,a,d,e' as _update_columns_ from uniquekey_partial_update_source_table;
select 'test partial update insert select operation, select2, from target table 1';
select * from uniquekey_partial_update1;

create table uniquekey_partial_update2 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 1, dedup_impl_version = 'dedup_in_txn_commit';
insert into uniquekey_partial_update2 (id, a, b, c, d, e, _update_columns_) select id, a, b, c, d, e, 'id,a,d,e' as _update_columns_ from uniquekey_partial_update_source_table settings optimize_unique_table_write = 1;
select 'test partial update insert select operation, select3, from target table 2';
select * from uniquekey_partial_update2;

-- create materialized view uniquekey_partial_update_view to uniquekey_partial_update2 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32), `_update_columns_` String) as select id, a, b, c, d, e, update_columns as _update_columns_ from uniquekey_partial_update_source_table;
-- insert into uniquekey_partial_update_source_table (id, a, d, update_columns) values (1, 4, [10, 20], 'id,d'), (2, 6, [50, 60], 'id,a');

-- select 'create materialized view, insert with partial update mode, enable merge_map_when_partial_update, enable unique row store, enable specify_update_columns_when_partial_update';

-- select * from uniquekey_partial_update_source_table order by id, a, b, c;
-- select * from uniquekey_partial_update1;

drop table if exists uniquekey_partial_update_source_table;
drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;
