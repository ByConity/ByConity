-- No virtual columns should be output in DESC TABLE query.

DROP TABLE IF EXISTS upyachka;
CREATE TABLE upyachka (x UInt64) Engine = CnchMergeTree ORDER BY tuple();

-- Merge table has virtual column `_table`
DESC TABLE merge(currentDatabase(0), 'upyachka');

DROP TABLE upyachka;
