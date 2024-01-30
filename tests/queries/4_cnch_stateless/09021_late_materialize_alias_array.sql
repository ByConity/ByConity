DROP TABLE IF EXISTS prewhere;
CREATE TABLE prewhere (x Array(UInt64), y ALIAS x, s String) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS enable_late_materialize = 1;
SELECT count() FROM prewhere WHERE (length(s) >= 1) = 0 AND NOT ignore(y);
DROP TABLE prewhere;
