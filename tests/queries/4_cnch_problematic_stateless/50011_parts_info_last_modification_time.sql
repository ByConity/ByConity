-- This test assume that GC will not removed dropped parts very often.

SELECT 'Insert, alter and drop table.';

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
    `name` String,
    `country` String,
    `asdf` Int
)
ENGINE = CnchMergeTree
PARTITION BY name
ORDER BY name;

-- Insert

INSERT INTO pi VALUES ('a', 'a', 1);

SELECT equals(
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ),
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);

-- Alter
ALTER TABLE pi DROP COLUMN asdf;

SYSTEM START MERGES pi;
OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;
SELECT number + sleepEachRow(3) from numbers(3) FORMAT Null;

SELECT equals(
    (
        SELECT count() FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ),
    (
       SELECT 2
    )
);

SELECT equals( 
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ), 
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);

-- Drop
TRUNCATE TABLE pi;

SELECT equals( 
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ), 
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);

---
SELECT 'Merge would not affect the last_modification_time.';

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
    `name` String,
    `country` String,
    `asdf` Int
)
ENGINE = CnchMergeTree
PARTITION BY name
ORDER BY name;

INSERT INTO pi VALUES ('a', 'a', 1);
INSERT INTO pi VALUES ('a', 'a', 1);
INSERT INTO pi VALUES ('a', 'a', 1);

SYSTEM STOP MERGES pi;
SYSTEM START MERGES pi;
OPTIMIZE TABLE pi SETTINGS mutations_sync = 1;

SELECT equals(
    (
        SELECT total_parts_number, total_rows_count FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    ),
    (
        SELECT 1, 3
    )
);
    
SELECT equals(
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi' and name like '%\_0\_%'
    ),
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);

---
SELECT 'Recalculate would not affect the correctness.';

TRUNCATE TABLE pi;

SELECT equals(
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ),
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);

SYSTEM RECALCULATE METRICS FOR pi;

SELECT number + sleepEachRow(3) from numbers(5) FORMAT Null;

SELECT equals(
    (
        SELECT max(commit_time) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = 'pi'
    ),
    (
        SELECT last_modification_time FROM system.cnch_parts_info WHERE database = currentDatabase() AND table = 'pi'
    )
);
