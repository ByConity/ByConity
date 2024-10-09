SET enable_optimizer=1;

DROP TABLE IF EXISTS test40074_map_keys;

CREATE TABLE test40074_map_keys(`id` Int32, `m` Map(String, Int32) KV)
    ENGINE = CnchMergeTree()
    PARTITION BY `id`
    PRIMARY KEY `id`
    ORDER BY `id`
    SETTINGS index_granularity = 8192;

insert into test40074_map_keys values (1, {'foo':1, 'bar':10});

-- { echo }
-- storage that support mapKeys/mapValues implicit column optimisation
explain select mapKeys(m) AS a, mapValues(m) AS b FROM test40074_map_keys;
select mapKeys(m) AS a, mapValues(m) AS b FROM test40074_map_keys;

-- storage that doesn't support mapKeys/mapValues implicit column optimisation
explain select Settings.Names, Settings.Values from system.processes;
