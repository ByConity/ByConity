CREATE TABLE test_query_cache_non_transactional (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

--- miss, hit, hit, hit, hit, hit ---
INSERT INTO test_query_cache_non_transactional values ('2019-01-01', 1, 'a');

sELECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 

SeLECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 

SElECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 

INSERT INTO test_query_cache_non_transactional values ('2019-01-01', 2, 'a')
SELeCT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 

SELEcT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 

SELECt * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0; 
DROP TABLE test_query_cache_non_transactional;

--- wait until data is flush into system table --
SYSTEM FLUSH LOGS;
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELeCT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELEcT * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECt * FROM test_query_cache_non_transactional ORDER BY id SETTINGS use_query_cache = 1, enable_transactional_query_cache = 0;' and current_database = currentDatabase(0);

