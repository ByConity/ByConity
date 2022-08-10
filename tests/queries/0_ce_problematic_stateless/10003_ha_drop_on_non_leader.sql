SET replication_alter_partitions_sync = 2, ignore_leader_check = 1, database_atomic_wait_for_drop_and_detach_synchronously = 1;

DROP TABLE IF EXISTS attach_r1;
DROP TABLE IF EXISTS attach_r2;

CREATE TABLE attach_r1 (d Date) ENGINE = HaMergeTree('/clickhouse/tables/test_10003/01/attach', 'r1', d, d, 8192);
INSERT INTO attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');
ALTER TABLE attach_r1 DROP PARTITION 201402;
SELECT d FROM attach_r1 ORDER BY d;
DROP TABLE attach_r1;

SELECT '---';

CREATE TABLE attach_r1 (d Date) ENGINE = HaMergeTree('/clickhouse/tables/test_10003/01/attach', 'r1') PARTITION BY d ORDER BY d SETTINGS ha_update_replica_stats_min_period = 1, ha_update_replica_stats_period = 1;
CREATE TABLE attach_r2 (d Date) ENGINE = HaMergeTree('/clickhouse/tables/test_10003/01/attach', 'r2') PARTITION BY d ORDER BY d SETTINGS ha_update_replica_stats_min_period = 1, ha_update_replica_stats_period = 1;

INSERT INTO attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');
SELECT d FROM attach_r1 ORDER BY d;

SELECT '---';

ALTER TABLE attach_r2 DROP PARTITION '2014-02-01';
SELECT sleep(1.0);
SELECT d FROM attach_r1 ORDER BY d;

DROP TABLE attach_r1;
DROP TABLE attach_r2;
