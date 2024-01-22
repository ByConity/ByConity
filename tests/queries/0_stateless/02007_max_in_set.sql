DROP TABLE IF EXISTS local_num;
DROP TABLE IF EXISTS global_num;

CREATE TABLE local_num (x INT) ENGINE = MergeTree() ORDER BY tuple(); 
CREATE TABLE global_num AS local_num ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_num); 
INSERT INTO local_num SELECT number FROM numbers(10);

SELECT number FROM numbers(5,15) WHERE number GLOBAL IN (SELECT x FROM global_num WHERE x < 12) SETTINGS max_rows_in_set=10;
SELECT number FROM numbers(5,15) WHERE number GLOBAL IN (SELECT x FROM global_num WHERE x < 12) SETTINGS max_bytes_in_set=10000;

SELECT number FROM numbers(5,15) WHERE number GLOBAL IN (SELECT x FROM global_num WHERE x < 12) SETTINGS max_rows_in_set=3; -- { serverError 191 }
SELECT number FROM numbers(5,15) WHERE number GLOBAL IN (SELECT x FROM global_num WHERE x < 12) SETTINGS max_bytes_in_set=10; -- { serverError 191 }
