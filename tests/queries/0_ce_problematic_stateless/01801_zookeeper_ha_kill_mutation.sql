-- "KILL MUTATION" will return database name, thus hardcode to "test" database in order to make the result deterministic
DROP TABLE IF EXISTS test.ha_kill_mutation_r1;
DROP TABLE IF EXISTS test.ha_kill_mutation_r2;

CREATE TABLE test.ha_kill_mutation_r1(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/test/ha_kill_mutation', '1') ORDER BY x PARTITION BY d;
CREATE TABLE test.ha_kill_mutation_r2(d Date, x UInt32, s String) ENGINE HaMergeTree('/clickhouse/tables/test/ha_kill_mutation', '2') ORDER BY x PARTITION BY d;

INSERT INTO test.ha_kill_mutation_r1 VALUES ('2000-01-01', 1, 'a');
INSERT INTO test.ha_kill_mutation_r1 VALUES ('2001-01-01', 2, 'b');

SELECT '*** Create and kill a single invalid mutation ***';

ALTER TABLE test.ha_kill_mutation_r1 FASTDELETE x WHERE toUInt32(s) = 1;
SELECT sleep(3) format Null;

SELECT mutation_id, latest_failed_part IN ('20000101_0_0_0', '20010101_1_1_0'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'ha_kill_mutation_r1';

KILL MUTATION WHERE database = 'test' AND table = 'ha_kill_mutation_r1';

SELECT count() FROM system.mutations WHERE database = 'test' AND table = 'ha_kill_mutation_r1';

SELECT '*** Create and kill invalid mutation that blocks another mutation ***';

SYSTEM SYNC REPLICA test.ha_kill_mutation_r1;
ALTER TABLE test.ha_kill_mutation_r1 FASTDELETE x WHERE toUInt32(s) = 1 SETTINGS mutation_query_id='invalid_mutation_r1';
ALTER TABLE test.ha_kill_mutation_r1 FASTDELETE s WHERE x = 1;
SELECT sleep(3) format Null;

SELECT mutation_id, latest_failed_part IN ('20000101_0_0_0', '20010101_1_1_0'), latest_fail_time != 0, substr(latest_fail_reason, 1, 8) FROM system.mutations WHERE database = 'test' AND table = 'ha_kill_mutation_r1' AND mutation_id = '0000000001';

KILL MUTATION WHERE database = 'test' AND table = 'ha_kill_mutation_r1' AND query_id = 'invalid_mutation_r1';
SYSTEM SYNC MUTATION test.ha_kill_mutation_r1;
SYSTEM SYNC MUTATION test.ha_kill_mutation_r2;

SELECT 'select r1';
SELECT * FROM test.ha_kill_mutation_r1;
SELECT 'select r2';
SELECT * FROM test.ha_kill_mutation_r2;

DROP TABLE test.ha_kill_mutation_r1;
DROP TABLE test.ha_kill_mutation_r2;
