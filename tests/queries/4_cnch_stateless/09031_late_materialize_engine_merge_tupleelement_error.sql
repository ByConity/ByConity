drop table if exists A1;
-- drop table if exists A_M;
CREATE TABLE A1( a DateTime ) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS enable_late_materialize = 1;
-- CREATE TABLE A_M as A1 ENGINE = Merge(currentDatabase(0), '^A1$');
insert into A1(a) select now();

-- SELECT tupleElement(arrayJoin([(1, 1)]), 1) FROM A_M PREWHERE tupleElement((1, 1), 1) =1;
--
-- SELECT tupleElement(arrayJoin([(1, 1)]), 1) FROM A_M WHERE tupleElement((1, 1), 1) =1;

SELECT tupleElement(arrayJoin([(1, 1)]), 1) FROM A1 PREWHERE tupleElement((1, 1), 1) =1;

drop table A1;
-- drop table A_M;
