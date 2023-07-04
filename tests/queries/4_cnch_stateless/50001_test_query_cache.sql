CREATE TABLE test_query_cache (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

--- miss, hit, hit again, miss, hit, hit again ---
INSERT INTO test_query_cache values ('2019-01-01', 1, 'a');
sELECT * FROM test_query_cache SETTINGS use_query_cache = 1; 

SeLECT * FROM test_query_cache SETTINGS use_query_cache = 1; 

SElECT * FROM test_query_cache SETTINGS use_query_cache = 1; 

INSERT INTO test_query_cache values ('2019-01-01', 2, 'a')
SELeCT * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1; 

SELEcT * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1; 

SELECt * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1; 
DROP TABLE test_query_cache;
--- wait until data is flush into system table --
SYSTEM FLUSH LOGS;
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT * FROM test_query_cache SETTINGS use_query_cache = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT * FROM test_query_cache SETTINGS use_query_cache = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT * FROM test_query_cache SETTINGS use_query_cache = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELeCT * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELEcT * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECt * FROM test_query_cache ORDER BY id SETTINGS use_query_cache = 1;';
