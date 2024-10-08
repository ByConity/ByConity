CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS arb;

CREATE TABLE arb
(
    id Int32,
    name String,
    birthday Nullable(Date),
    gid UInt32
)
ENGINE = CnchMergeTree()
PARTITION BY id
ORDER BY id;

INSERT INTO arb VALUES (1, 'Anna', '2001-01-01', 1), (2, 'Bob', '2002-02-02', 1), (3, 'Charlie', '2003-03-03', 1), (4, 'Dan', NULL, 2), (5, 'Eve', '2004-04-04', 2), (6, 'Frans', NULL, 3);

SELECT(SELECT arbitrary(id) FROM arb) >= 1;
SELECT(SELECT arbitrary(id) FROM arb) <= 6;
SELECT(SELECT arbitrary(id) FROM arb) < 1;
SELECT(SELECT arbitrary(id) FROM arb) > 6;

SELECT(SELECT arbitrary(name) FROM arb WHERE id <= 3) IN ('Anna', 'Bob', 'Charlie');
SELECT(SELECT arbitrary(name) FROM arb WHERE id <= 3) NOT IN ('Dan', 'Eve', 'Frans');
SELECT(SELECT arbitrary(name) FROM arb WHERE id == 4 OR id == 5) IN ('Dan', 'Eve');
SELECT(SELECT arbitrary(name) FROM arb WHERE id == 4 OR id == 5) NOT IN ('Anna', 'Bob', 'Charlie', 'Frans');
SELECT(SELECT arbitrary(name) FROM arb WHERE id == 6) IN ('Frans');
SELECT(SELECT arbitrary(name) FROM arb WHERE id == 6) NOT IN ('Anna', 'Bob', 'Charlie', 'Dan','Eve');

SELECT(SELECT arbitrary(birthday) FROM arb WHERE gid = 1) IN ('2001-01-01', '2002-02-02', '2003-03-03');
SELECT(SELECT arbitrary(birthday) FROM arb WHERE gid = 1) NOT IN ('2004-04-04', NULL);
SELECT(SELECT arbitrary(birthday) FROM arb WHERE gid = 2) == '2004-04-04';
SELECT(SELECT arbitrary(birthday) FROM arb WHERE gid = 3);

SELECT(SELECT arbitrary(gid) FROM arb WHERE id <= 3) == 1;
SELECT(SELECT arbitrary(gid) FROM arb WHERE 4 == id OR id == 5) == 2;
SELECT(SELECT arbitrary(gid) FROM arb WHERE id == 6) == 3;

