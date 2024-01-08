DROP TABLE IF EXISTS ttl_old_syntax_compatible;

CREATE TABLE ttl_old_syntax_compatible (p_date Date, i Int) ENGINE = CnchMergeTree PARTITION BY toDate(p_date) ORDER BY i TTL `toDate(p_date)` + toIntervalDay(10);

DROP TABLE ttl_old_syntax_compatible;