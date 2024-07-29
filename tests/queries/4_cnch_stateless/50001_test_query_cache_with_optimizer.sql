CREATE TABLE left_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

CREATE TABLE right_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

--- miss, hit, hit again,  ---
INSERT INTO left_table values ('2019-01-01', 1, 'a');
INSERT INTO right_table values ('2019-01-01', 1, 'b');
--- sleep to make sure insert transaction is commited ---
SELECT sleepEachRow(3) FROM numbers(2) FORMAT Null;
sELECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;

SeLECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

SElECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- miss, hit, hit again ---
INSERT INTO left_table values ('2019-01-01', 2, 'c');
--- sleep to make sure insert transaction is commited ---
SELECT sleepEachRow(3) FROM numbers(2) FORMAT Null;
SELeCT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

SELEcT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

SELECt a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- miss, hit, hit again --
INSERT INTO right_table values ('2019-01-01', 2, 'd');
--- sleep to make sure insert transaction is commited ---
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT a, r.a From left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

SELECT a, r.a fRom left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

SELECT a, r.a frOm left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

DROP TABLE left_table;
DROP TABLE right_table;
--- wait until data is flush into system table --
SYSTEM FLUSH LOGS;
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELeCT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELEcT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECt a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a From left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a fRom left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a frOm left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;' and current_database = currentDatabase(0);
