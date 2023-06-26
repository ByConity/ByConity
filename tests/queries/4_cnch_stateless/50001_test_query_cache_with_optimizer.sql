CREATE TABLE left_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

CREATE TABLE right_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

INSERT INTO left_table values ('2019-01-01', 1, 'a');
INSERT INTO right_table values ('2019-01-01', 1, 'b');
--- miss ---
sELECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;

--- hit ---
SeLECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- hit again ---
SElECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

INSERT INTO left_table values ('2019-01-01', 2, 'c');
--- miss ---
SELeCT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- hit ---
SELEcT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- hit again ---
SELECt a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

INSERT INTO right_table values ('2019-01-01', 2, 'd');
--- miss --
SELECT a, r.a From left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- hit ---
SELECT a, r.a fRom left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

--- hit again ---
SELECT a, r.a frOm left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1; 

DROP TABLE left_table;
DROP TABLE right_table;
--- wait until data is flush into system table --
SELECT sleep(3) Format Null;
SELECT sleep(3) Format Null;
SELECT sleep(3) Format Null;
SELECT sleep(3) Format Null;
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELeCT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELEcT a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECt a, r.a from left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a From left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a fRom left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SELECT a, r.a frOm left_table as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 1;';
