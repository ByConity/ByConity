CREATE TABLE left_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

CREATE TABLE right_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    ORDER BY `id`;

--- all miss ---
INSERT INTO left_table values ('2019-01-01', 1, 'a');
INSERT INTO right_table values ('2019-01-01', 1, 'b');

CREATE VIEW left_table_view AS SELECT * FROM left_table;

sELECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;
SeLECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;
SElECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;

sELECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;
SeLECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;
SElECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;

DROP TABLE left_table_view;
DROP TABLE left_table;
DROP TABLE right_table;
--- wait until data is flush into system table --
SYSTEM FLUSH LOGS;
SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT * from left_table_view Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'sELECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SeLECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);

SELECT ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheHits')] as h, ProfileEvents.Values[indexOf(ProfileEvents.Names, 'QueryCacheMisses')] as m FROM cnch(server, system.query_log) WHERE event_date = today() and type = 'QueryFinish' and query = 'SElECT a, r.a from left_table_view as l INNER JOIN right_table as r on l.id = r.id ORDER BY id Settings use_query_cache = 1, enable_optimizer = 0;' and current_database = currentDatabase(0);
