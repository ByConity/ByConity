DROP TABLE IF EXISTS defaults_on_defaults;
CREATE TABLE defaults_on_defaults (
    key UInt64
)
ENGINE = CnchMergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 3, enable_late_materialize = 1;

SET late_materialize_aggressive_push_down = 1;

INSERT INTO defaults_on_defaults values (1) (2) (3) (4) (5);

-- OPTIMIZE TABLE defaults_on_defaults;

ALTER TABLE defaults_on_defaults ADD COLUMN `Arr.C1` Array(UInt32) DEFAULT emptyArrayUInt32();

SELECT 1 FROM defaults_on_defaults WHERE length(`Arr.C1`) = 0;

ALTER TABLE defaults_on_defaults ADD COLUMN `Arr.C2` Array(UInt32) DEFAULT arrayResize(emptyArrayUInt32(), length(Arr.C1));

ALTER TABLE defaults_on_defaults ADD COLUMN `Arr.C3` Array(UInt32) ALIAS arrayResize(emptyArrayUInt32(), length(Arr.C2));

SELECT 2 from defaults_on_defaults where length(`Arr.C2`) = 0;

SELECT 3 from defaults_on_defaults where length(`Arr.C3`) = 0;

ALTER TABLE defaults_on_defaults ADD COLUMN `Arr.C4` Array(UInt32) DEFAULT arrayResize(emptyArrayUInt32(), length(Arr.C3));

SELECT 4 from defaults_on_defaults where length(`Arr.C4`) = 0;

ALTER TABLE defaults_on_defaults ADD COLUMN `ArrLen` UInt64 DEFAULT length(Arr.C4);

SELECT 5 from defaults_on_defaults where ArrLen = 0;

SELECT * from defaults_on_defaults where ArrLen = 0;

SHOW CREATE TABLE defaults_on_defaults;

-- OPTIMIZE TABLE defaults_on_defaults FINAL;

SELECT 6 from defaults_on_defaults where length(`Arr.C4`) = 0;

DROP TABLE IF EXISTS defaults_on_defaults;
