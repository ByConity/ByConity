DROP TABLE IF EXISTS versioned_collapsing;

CREATE TABLE versioned_collapsing (
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8 NOT NULL,
    Version UInt8 NOT NULL
)
Engine=CnchVersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
;

INSERT INTO versioned_collapsing VALUES (1, 1, 1, 1, 1);
INSERT INTO versioned_collapsing VALUES (1, 1, 1, -1, 1);
INSERT INTO versioned_collapsing VALUES (2, 2, 2, 1, 2);
INSERT INTO versioned_collapsing VALUES (2, 2, 2, -1, 2);
INSERT INTO versioned_collapsing VALUES (3, 3, 3, 1, 3);
SYSTEM START MERGES versioned_collapsing;
SELECT 'Background thread for versioned_collapsing';
SELECT count() > 0 FROM system.bg_threads where database = currentDatabase(0) and table = 'versioned_collapsing';
OPTIMIZE table versioned_collapsing SETTINGS mutations_sync = 1; 

DROP TABLE IF EXISTS collapsing;
CREATE TABLE collapsing (
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8 NOT NULL
)
Engine=CnchCollapsingMergeTree(Sign)
ORDER BY UserID
;

INSERT INTO collapsing VALUES (1, 1, 1, 1);
INSERT INTO collapsing VALUES (1, 1, 1, -1);
INSERT INTO collapsing VALUES (2, 2, 2, 1);
INSERT INTO collapsing VALUES (2, 2, 2, -1);
INSERT INTO collapsing VALUES (3, 3, 3, 1);
SYSTEM START MERGES collapsing;
SELECT 'Background thread for collapsing';
SELECT count() > 0 FROM system.bg_threads where database = currentDatabase(0) and table = 'collapsing';
OPTIMIZE table collapsing SETTINGS mutations_sync = 1; 

DROP TABLE IF EXISTS replacing;
CREATE TABLE replacing (
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Version Int8 NOT NULL
)
Engine=CnchReplacingMergeTree(Version)
ORDER BY UserID
;

INSERT INTO replacing VALUES (1, 1, 1, 1);
INSERT INTO replacing VALUES (1, 1, 1, 2);
INSERT INTO replacing VALUES (1, 2, 2, 3);
INSERT INTO replacing VALUES (1, 2, 2, 4);
INSERT INTO replacing VALUES (1, 3, 3, 5);
SYSTEM START MERGES replacing;
SELECT 'Background thread for replacing';
SELECT count() > 0 FROM system.bg_threads where database = currentDatabase(0) and table = 'replacing';
OPTIMIZE table replacing SETTINGS mutations_sync = 1; 

DROP TABLE IF EXISTS summing;
CREATE TABLE summing (
    UserID UInt64,
    PageViews UInt64,
    Duration UInt64
)
Engine=CnchSummingMergeTree((PageViews, Duration))
ORDER BY UserID
;

INSERT INTO summing VALUES (1, 1, 1);
INSERT INTO summing VALUES (1, 1, 1);
INSERT INTO summing VALUES (1, 2, 2);
INSERT INTO summing VALUES (1, 2, 2);
INSERT INTO summing VALUES (1, 3, 3);
SYSTEM START MERGES summing;
SELECT 'Background thread for summing';
SELECT count() > 0 FROM system.bg_threads where database = currentDatabase(0) and table = 'summing';
OPTIMIZE table summing SETTINGS mutations_sync = 1; 

DROP TABLE IF EXISTS aggregating;
CREATE TABLE aggregating
(
    t DateTime,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = CnchAggregatingMergeTree()
PARTITION BY toYYYYMMDD(t)
ORDER BY t;

SYSTEM START MERGES aggregating;
SELECT 'Background thread for aggregating';
SELECT count() > 0 FROM system.bg_threads where database = currentDatabase(0) and table = 'aggregating';

-- change to select * later after support mutation sync --
SELECT count() > 0 FROM versioned_collapsing;
SELECT count() > 0 FROM collapsing;
SELECT count() > 0 FROM replacing;
SELECT count() > 0 FROM summing;

DROP TABLE IF EXISTS versioned_collapsing;
DROP TABLE IF EXISTS collapsing;
DROP TABLE IF EXISTS replacing;
DROP TABLE IF EXISTS summing;
DROP TABLE IF EXISTS aggregating;
