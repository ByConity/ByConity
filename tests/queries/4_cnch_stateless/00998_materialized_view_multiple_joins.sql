drop database if exists test00998mv;
create database test00998mv;

DROP TABLE IF EXISTS test00998mv.view_join;
DROP TABLE IF EXISTS test00998mv.view_subquery;
DROP TABLE IF EXISTS test00998mv.source;
DROP TABLE IF EXISTS test00998mv.target_subquery;
DROP TABLE IF EXISTS test00998mv.target_join;
DROP TABLE IF EXISTS test00998mv.dim;


CREATE TABLE test00998mv.source  (`id` Int32, `s` String) ENGINE = CnchMergeTree PARTITION BY id % 2 ORDER BY id;
CREATE TABLE test00998mv.target_subquery  (`id` Int32, `s` String) ENGINE = CnchMergeTree PARTITION BY id % 2 ORDER BY id;
CREATE TABLE test00998mv.target_join  (`id` Int32, `s` String) ENGINE = CnchMergeTree PARTITION BY id % 2 ORDER BY id;
CREATE TABLE test00998mv.dim (`s` String) ENGINE = CnchMergeTree ORDER BY s;

INSERT INTO test00998mv.source select number, IF(number%2=1, 'a', 'b') from numbers(100);
INSERT INTO test00998mv.dim select 'a';

CREATE MATERIALIZED VIEW test00998mv.view_subquery TO test00998mv.target_subquery (`id` Int32, `s` String) AS SELECT id, s FROM test00998mv.source  WHERE s IN (SELECT DISTINCT s FROM test00998mv.dim );

CREATE MATERIALIZED VIEW test00998mv.view_join TO test00998mv.target_join (`id` Int32, `s` String) AS SELECT t.id, t.s FROM test00998mv.source as t join test00998mv.dim as d on t.s = d.s;

refresh MATERIALIZED VIEW test00998mv.view_subquery partition '1';

refresh MATERIALIZED VIEW test00998mv.view_join partition '1';


select count() from test00998mv.target_subquery;
select count() from test00998mv.target_join;

truncate table test00998mv.target_subquery;
truncate table test00998mv.target_join;
truncate table test00998mv.source;

INSERT INTO test00998mv.source select number, IF(number%2=1, 'a', 'b') from numbers(100,200);
select count() from test00998mv.target_subquery;
select count() from test00998mv.target_join;
select count() from test00998mv.source;

DROP TABLE test00998mv.view_join;;
DROP TABLE test00998mv.view_subquery;
DROP TABLE test00998mv.source;
DROP TABLE test00998mv.target_subquery;
DROP TABLE test00998mv.target_join;
DROP TABLE test00998mv.dim;

drop database test00998mv;