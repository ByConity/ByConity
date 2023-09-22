CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS t;

CREATE TABLE t(a Int32, `string_params` Map(String, String))
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`
    SETTINGS index_granularity = 8192;

insert into t select a, string_params from t;

DROP TABLE IF EXISTS t;
