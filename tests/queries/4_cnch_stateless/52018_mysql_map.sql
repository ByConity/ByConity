drop table if exists 52018_mysql_map;

set dialect_type = 'MYSQL';
set enable_implicit_arg_type_convert = 1;

create table 52018_mysql_map (a Nullable(int), m1 Map(String, String) not NULL BYTE, m2 Map(String, String) not NULL, m3 Map(String, String), m4 Map(String, Map(String, String)) not NULL) engine=CnchMergeTree() order by a;

insert into 52018_mysql_map values (0, {'k1': 'v1'}, {'k1': 'v1'}, {'k1': 'v1'}, {'k1': {}}) (2, {'k1': 'v1'}, {'k1': 'v1'}, {'k1': 'v1'}, {'k1': {'k1': 'v1'}}) (NULL, {}, {}, NULL, {});

select 'element_at';
select element_at(m1, NULL) e1, element_at(m2, NULL) e2, element_at(m3, NULL) e3, element_at(m4, NULL) as e4, toTypeName(e1), toTypeName(e2), toTypeName(e3), toTypeName(e4) from 52018_mysql_map order by a;
select element_at(m1, 'k1') e1, element_at(m2, 'k1') e2, element_at(m3, 'k1') e3, element_at(m4, 'k1') as e4, toTypeName(e1), toTypeName(e2), toTypeName(e3), toTypeName(e4) from 52018_mysql_map order by a;
select element_at(m1, 'k2') e1, element_at(m2, 'k2') e2, element_at(m3, 'k2') e3, element_at(m4, 'k2') as e4, toTypeName(e1), toTypeName(e2), toTypeName(e3), toTypeName(e4) from 52018_mysql_map order by a;

select element_at(NULL, '5');
select element_at(NULL, 5);
select element_at(map(), NULL);
select element_at(map('s1', 'v1'), NULL);
select element_at(NULL, NULL);
select element_at(string_map, 's1') from (select NULL as string_map);
select element_at(map(), s1) from (select NULL as s1);
select element_at(string_map, s1) from (select NULL as string_map, NULL as s1);

WITH map(1, 2, 3, NULL) AS m SELECT element_at(m, toNullable(1)), element_at(m, toNullable(2)), element_at(m, toNullable(3));
WITH map(1, 2, 3, NULL) AS m SELECT element_at(m, materialize(toNullable(1))), element_at(m, materialize(toNullable(2))), element_at(m, materialize(toNullable(3)));
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT element_at(m, toNullable(1)), element_at(m, toNullable(2)), element_at(m, toNullable(3));
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT element_at(m, materialize(toNullable(1))), element_at(m, materialize(toNullable(2))), element_at(m, materialize(toNullable(3)));

WITH map('a', 2, 'b', NULL) AS m SELECT element_at(m, toNullable('a')), element_at(m, toNullable('b')), element_at(m, toNullable('c'));
WITH map('a', 2, 'b', NULL) AS m SELECT element_at(m, materialize(toNullable('a'))), element_at(m, materialize(toNullable('b'))), element_at(m, materialize(toNullable('c')));
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT element_at(m, toNullable('a')), element_at(m, toNullable('b')), element_at(m, toNullable('c'));
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT element_at(m, materialize(toNullable('a'))), element_at(m, materialize(toNullable('b'))), element_at(m, materialize(toNullable('c')));

WITH map(1, 2, 3, NULL) AS m SELECT element_at(m, 1), element_at(m, 2), element_at(m, 3);
WITH map(1, 2, 3, NULL) AS m SELECT element_at(m, materialize(1)), element_at(m, materialize(2)), element_at(m, materialize(3));
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT element_at(m, 1), element_at(m, 2), element_at(m, 3);
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT element_at(m, materialize(1)), element_at(m, materialize(2)), element_at(m, materialize(3));

WITH map('a', 2, 'b', NULL) AS m SELECT element_at(m, 'a'), element_at(m, 'b'), element_at(m, 'c');
WITH map('a', 2, 'b', NULL) AS m SELECT element_at(m, materialize('a')), element_at(m, materialize('b')), element_at(m, materialize('c'));
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT element_at(m, 'a'), element_at(m, 'b'), element_at(m, 'c');
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT element_at(m, materialize('a')), element_at(m, materialize('b')), element_at(m, materialize('c'));

-- argument 1 is non-const nullable
WITH materialize(map(0, 1)) AS m, materialize(cast(0, 'Nullable(Int)')) AS k SELECT arrayElement(m, k) AS r, toTypeName(r);
WITH materialize(map(0, 1)) AS m, materialize(cast(1, 'Nullable(Int)')) AS k SELECT arrayElement(m, k) AS r, toTypeName(r);
WITH materialize(map(0, 1)) AS m, materialize(cast(NULL, 'Nullable(Int)')) AS k SELECT arrayElement(m, k) AS r, toTypeName(r);


select 'map_keys';
select map_keys(m1), map_keys(m2), map_keys(m3), map_keys(m4) from 52018_mysql_map;
select map_keys(map());
select map_keys(NULL);


select 'map_values';
select map_values(m1), map_values(m2), map_values(m3), map_values(m4) from 52018_mysql_map;
select map_values(map());
select map_values(NULL);

select 'size';
select size(m1), size(m2), size(m3), size(m4) from 52018_mysql_map;
select size(map());
select size(NULL);

drop table 52018_mysql_map;
