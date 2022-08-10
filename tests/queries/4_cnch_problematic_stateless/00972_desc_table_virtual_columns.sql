-- No virtual columns should be output in DESC TABLE query.
use default;
DROP TABLE IF EXISTS upyachka;
CREATE TABLE upyachka (x UInt64) ENGINE = Memory;

-- Merge table has virtual column `_table`
DESC TABLE merge(currentDatabase(), 'upyachka');

DROP TABLE upyachka;
