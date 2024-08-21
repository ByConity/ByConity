set dialect_type='ANSI';
set data_type_default_nullable=false;
set enable_optimizer_for_create_select=1;

DROP TABLE if exists multi_subquery_source;
DROP TABLE if exists multi_subquery_target;

CREATE TABLE multi_subquery_source
(
    p_date Date,
    c1 Nullable(Int64),
    c2 Nullable(Int64),
    str_map Map(String, String)
)
ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY p_date;

INSERT INTO multi_subquery_source values('2024-08-17', 1, 2, {'k1': 'v1'});

SELECT getMapKeys(currentDatabase(), 'multi_subquery_source', 'str_map', '.*2024-*08-*17.*', 60);
SELECT length(arrayFilter(x -> x = (SELECT c1 FROM multi_subquery_source), range(10))), length(arrayFilter(x -> x = (SELECT c2 FROM multi_subquery_source), range(10)));

CREATE TABLE multi_subquery_target ENGINE = CnchMergeTree ORDER BY tuple() AS (SELECT getMapKeys(currentDatabase(), 'multi_subquery_source', 'str_map', '.*2024.*08.*17.*', 60)); -- { serverError 81 }
DROP TABLE multi_subquery_target;

CREATE TABLE multi_subquery_target ENGINE = CnchMergeTree ORDER BY tuple() AS (SELECT length(arrayFilter(x -> x = (SELECT c1 FROM multi_subquery_source), range(10))), length(arrayFilter(x -> x = (SELECT c2 FROM multi_subquery_source), range(10))));

SELECT * FROM multi_subquery_target;

DROP TABLE multi_subquery_source;
DROP TABLE multi_subquery_target;
