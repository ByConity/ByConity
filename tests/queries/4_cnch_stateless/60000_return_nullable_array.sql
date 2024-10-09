drop table if exists test_array_null;
create table test_array_null
(
    id UInt32
)
engine=CnchMergeTree
order by id;
insert into test_array_null values (1);
-- Nested type Map(UInt8, UInt8) cannot be inside Nullable type
select indexOf(arraySort(groupUniqArray(if(id = 0, NULL, id))), 1) as pos_nick, toTypeName(pos_nick), arrayElement(array(map(1,2)), pos_nick) as nick, toTypeName(nick) from test_array_null;
set allow_return_nullable_array = 0;
select indexOf(arraySort(groupUniqArray(if(id = 0, NULL, id))), 1) as pos_nick, toTypeName(pos_nick), arrayElement(array(map(1,2)), pos_nick) as nick, toTypeName(nick) from test_array_null;
drop table test_array_null;
