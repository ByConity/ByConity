set dialect_type='MYSQL';
use test;
drop table if exists 60004_t1;
create table 60004_t1(id Int64 auto_increment, val String);
SHOW CREATE TABLE 60004_t1;

INSERT INTO 60004_t1(val) VALUES ('Value 1'), ('Value 2'), ('Value 3');

SELECT (SELECT id AS id1 FROM 60004_t1 WHERE val = 'Value 1')  < (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value 2') AS comparison;

SELECT (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value 2')  < (SELECT id AS id3 FROM 60004_t1 WHERE val = 'Value 3') AS comparison;

INSERT INTO 60004_t1(id, val) VALUES (100, 'Explicit ID 100'), (101, 'Explicit ID 101');

SELECT id AS id1 FROM 60004_t1 WHERE val = 'Explicit ID 100';
SELECT id AS id1 FROM 60004_t1 WHERE val = 'Explicit ID 101';

INSERT INTO 60004_t1(val) VALUES ('Value after explicit ID');

SELECT (SELECT id AS id1 FROM 60004_t1 WHERE val = 'Value 1')  < (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value after explicit ID') AS comparison;

INSERT INTO 60004_t1(val) VALUES ('Value 4'), ('Value 5'), ('Value 6');

SELECT (SELECT id AS id1 FROM 60004_t1 WHERE val = 'Value 3')  < (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value 4') AS comparison;

SELECT (SELECT id AS id1 FROM 60004_t1 WHERE val = 'Value 4')  < (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value 5') AS comparison;

SELECT (SELECT id AS id2 FROM 60004_t1 WHERE val = 'Value 5')  < (SELECT id AS id3 FROM 60004_t1 WHERE val = 'Value 6') AS comparison;

drop table if exists 60004_t1;
