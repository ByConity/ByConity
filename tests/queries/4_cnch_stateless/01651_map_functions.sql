set allow_experimental_map_type = 1;

-- String type
drop table if exists table_map;
create table table_map (a Map(String, String), b String) engine = CnchMergeTree order by tuple();
insert into table_map values ({'name':'zhangsan', 'age':'10'}, 'name'), ({'name':'lisi', 'gender':'female'},'age');
select mapContains(a, 'name') from table_map;
select mapContains(a, 'gender') from table_map;
select mapContains(a, 'abc') from table_map;
select mapContains(a, b) from table_map;
select mapContains(a, 10) from table_map; -- { serverError 43 }
select arraySort(mapKeys(a)) from table_map;
drop table if exists table_map;

CREATE TABLE table_map (a Map(UInt8, Int), b UInt8, c UInt32) engine = CnchMergeTree order by tuple();
insert into table_map select map(number, number), number, number from numbers(1000, 3);
select mapContains(a, b), mapContains(a, c), mapContains(a, 233) from table_map;
select mapContains(a, 'aaa') from table_map; -- { serverError 43 }
select mapContains(b, 'aaa') from table_map; -- { serverError 43 }
select arraySort(mapKeys(a)) from table_map;
select arraySort(mapValues(a)) from table_map;
drop table if exists table_map;


-- Const column
select map( 'aa', 4, 'bb' , 5) as m, mapKeys(m), mapValues(m);
select map( 'aa', 4, 'bb' , 5) as m, mapContains(m, 'aa'), mapContains(m, 'k');

select map(0, 0) as m, mapContains(m, number % 2) from numbers(2);

create table table_map (a Int64, b Nullable(String)) ENGINE = CnchMergeTree ORDER BY a;
insert into table_map values (1, null) (2, 's1');
select map('s1', 'v1', 's2', 'v2'){b} from table_map; -- { serverError 43 }
select map('s1', 'v1', 's2', 'v2')[b] from table_map;
drop table if exists table_map;

create table table_map (a Int64, b String) ENGINE = CnchMergeTree ORDER BY a;
insert into table_map values (1, null) (2, 's1');
select map('s1', 'v1', 's2', 'v2'){b} from table_map;
select map('s1', 'v1', 's2', 'v2')[b] from table_map;
drop table if exists table_map;


-- mapElement with null arguments
select mapElement(NULL, '5');
select mapElement(NULL, 5);
select mapElement(map(), NULL);
select mapElement(map('s1', 'v1'), NULL);
select mapElement(NULL, NULL);
select string_map{'s1'} from (select NULL as string_map);
select map(){s1} from (select NULL as s1);
select string_map{s1} from (select NULL as string_map, NULL as s1);
