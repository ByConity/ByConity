set enable_unique_partial_update = 1;
drop table if exists unique_partial_update_alter;

CREATE table unique_partial_update_alter(
    `id` UInt64,
    `s1` String,
    `a1` Array(Int32),
    `m1` Map(String, Int32))
ENGINE = CnchMergeTree()
order by id
unique key id
SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 1;

insert into unique_partial_update_alter values (1, '', [1, 2], {'k1': 1, 'k2': 2}), (2, 'abc', [3, 4 ,5], {'k3': 3, 'k4': 4});
insert into unique_partial_update_alter values (1, 'bce', [10, 20], {'k1': 10, 'k5': 5}), (2, '', [], {'k7': 7, 'k8': 8});

select 'insert with partial update mode';
select * from unique_partial_update_alter order by id;

alter table unique_partial_update_alter drop column a1, add column a2 Array(Int32);
insert into unique_partial_update_alter values (1, '', {'k5': 50, 'k9': 9}, [100, 200]), (3, 'def', {'k3': 4, 'k4': 3}, [30, 40, 50]);

select * from unique_partial_update_alter order by id;

insert into unique_partial_update_alter values (2, 'bcd', {'k5': 5}, [10, 20]), (5, 'efg', {'k7': 7}, [70, 80]);
select 'update 1 row and insert one row';
select * from unique_partial_update_alter order by id;

alter table unique_partial_update_alter modify column a2 String;
insert into unique_partial_update_alter values (5, 'hij', {'k7': 70}, '100, 200'), (7, 'xyg', {'k8': 8}, '10010');
select 'modify column a1, update one row and insert one new row';
select * from unique_partial_update_alter order by id;

alter table unique_partial_update_alter modify SETTING enable_delete_mutation_on_unique_table = 1;

DELETE FROM unique_partial_update_alter where id <= 2;
insert into unique_partial_update_alter values (3, 'opq', {'k3': 30}, '30, 40'), (7, 'xyg', {'k8': 80}, '20010');
select 'delete two rows, then update one row and insert one new row';
select * from unique_partial_update_alter order by id;

drop table if exists unique_partial_update_alter;
