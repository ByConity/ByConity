DROP TABLE IF EXISTS test_query_cache_alter;
CREATE TABLE test_query_cache_alter (d Date, id UInt64, t UInt32)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

--- disable merge thread to prevent alter task running ---
SYSTEM STOP MERGES test_query_cache_alter;
--- miss, hit, hit again, miss, hit, hit again ---
INSERT INTO test_query_cache_alter values ('2019-01-01', 1, 1024);

--- sleep to make sure insert transaction is commited ---
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

sELECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 

SeLECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 

SElECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 

ALTER TABLE test_query_cache_alter MODIFY COLUMN t UInt8;
SELeCT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 

SELEcT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 

SELECt * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1; 
DROP TABLE test_query_cache_alter;
--- wait until data is flush into system table --
SYSTEM FLUSH LOGS;
SELECT '1st';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
SELECT '2nd';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
SELECT '3rd';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
SELECT '4th';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELeCT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
SELECT '5th';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELEcT * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
SELECT '6th';
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECt * FROM test_query_cache_alter ORDER BY id SETTINGS use_query_cache = 1;' and current_database = currentDatabase(0);
