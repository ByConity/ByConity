CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS gc;

CREATE TABLE gc
(
    id Int32,
    name String,
    birthday Nullable(Date),
    gid UInt32
)
ENGINE = CnchMergeTree()
PARTITION BY id
ORDER BY id;

INSERT INTO gc VALUES (1, 'Anna', '2001-01-01', 1), (2, 'Bob', '2002-02-02', 1), (3, 'Charlie', '2003-03-03', 1), (4, 'Dan', NULL, 2), (5, 'Eve', '2004-04-04', 2), (6, 'Frans', NULL, 3), (7, 'Anna', NULL, 4);

SELECT groupConcat(name) FROM (SELECT * FROM gc ORDER BY id);
SELECT groupConcat(name) FROM (SELECT * FROM gc ORDER BY id) GROUP BY gid ORDER BY gid;
SELECT groupConcat('#')(name) FROM (SELECT * FROM gc ORDER BY id);
SELECT groupConcat('+')(DISTINCT name) FROM (SELECT * FROM gc ORDER BY id);
SELECT groupConcat('#') FROM (SELECT * FROM gc ORDER BY id);
SELECT groupConcat(DISTINCT '#') FROM gc;

SELECT GROUP_CONCAT(DISTINCT name SEPARATOR '#') FROM (SELECT * FROM gc ORDER BY id);

SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC SEPARATOR '#') FROM gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id ASC SEPARATOR '#') FROM gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id DESC SEPARATOR '#') FROM gc;
SELECT GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id ASC SEPARATOR '#'), GROUP_CONCAT(DISTINCT name ORDER BY name DESC, id DESC SEPARATOR '#') FROM gc;

-- Mysql compatibility
SELECT GROUP_CONCAT(DISTINCT name ORDER BY BINARY name DESC SEPARATOR '#') FROM gc;

-- More uses of group_concat
create table 01628_group_concat_aggregate_function (grp int, a bigint unsigned, c char(10) not null, d char(10) not null) ENGINE = CnchMergeTree ORDER BY tuple();
insert into 01628_group_concat_aggregate_function values (1,1,'a','a');
insert into 01628_group_concat_aggregate_function values (2,2,'b','a');
insert into 01628_group_concat_aggregate_function values (2,3,'c','b');
insert into 01628_group_concat_aggregate_function values (3,4,'E','a');
insert into 01628_group_concat_aggregate_function values (3,5,'C','b');
insert into 01628_group_concat_aggregate_function values (3,6,'D','b');
insert into 01628_group_concat_aggregate_function values (3,7,'d','d');
insert into 01628_group_concat_aggregate_function values (3,8,'d','d');
insert into 01628_group_concat_aggregate_function values (3,9,'D','c');

select grp, group_concat(a,c order by a,c) from 01628_group_concat_aggregate_function group by grp order by grp;
select grp, group_concat('(',a,':',c,')' order by a,c) from 01628_group_concat_aggregate_function group by grp order by grp;
SET dialect_type='MYSQL'; -- implicit conversion of `plus` operands requires mysql dialect
select grp, group_concat(a order by d+c-ascii(c),a) from 01628_group_concat_aggregate_function group by grp order by grp;
select grp, group_concat(a order by a separator '')+0 from 01628_group_concat_aggregate_function group by grp order by grp;
select grp, group_concat(a order by a separator '')+0.0 from 01628_group_concat_aggregate_function group by grp order by grp;
select grp, ROUND(group_concat(a order by a separator '')) from 01628_group_concat_aggregate_function group by grp order by grp;


