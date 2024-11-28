DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test
(
    s String NOT NULL,
    arr Array(UInt8)
) ENGINE = CnchMergeTree() order by s;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);

SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr2
SETTINGS enable_optimizer = 0;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr
SETTINGS enable_optimizer = 0;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr
SETTINGS enable_optimizer = 0;
select '---';
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr2
SETTINGS enable_optimizer = 1;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr AS arr
SETTINGS enable_optimizer = 1;
SELECT
    s,
    arr
FROM arrays_test
ARRAY JOIN arr
SETTINGS enable_optimizer = 1;

SELECT name, attribute.names FROM (select 'name' as name, [1] as `attribute.names`) as t1 ARRAY JOIN attribute;

SELECT name , att.names,`attribute.types` FROM (select 'name' as name, [1,2] as `attribute.names`,[2,3] `attribute.types`) as t1 ARRAY JOIN attribute as att;

SELECT name,attribute.names,`attribute.types`,arr FROM (select 'name' as name, [1,2] as `attribute.names`,[3] `attribute.types`) as t1 ARRAY JOIN arrayPopBack(`attribute.names`) as arr;

SELECT name , att.names, `attribute.names`, `attribute.types` FROM (select 'name' as name, [1,2] as `attribute.names`,[2,3] `attribute.types`) as t1 ARRAY JOIN attribute as att, `attribute.names`;

select name,`attribute.names`,`attribute.types`, `att.names`, `att.types` FROM (select 'name' as name, [1,2] as `attribute.names`,[1,2] `attribute.types`) as t1 ARRAY JOIN attribute, attribute as att;
