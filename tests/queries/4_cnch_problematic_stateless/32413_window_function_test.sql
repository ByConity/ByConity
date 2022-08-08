CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.people;
CREATE TABLE test.people (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() PRIMARY KEY id order by id;

INSERT INTO test.people VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO test.people VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO test.people VALUES('3', 'data', '2019-02-01', '22');
INSERT INTO test.people VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO test.people VALUES('5', 'data', '2019-04-01', '24');
INSERT INTO test.people VALUES('6', 'payment', '2019-01-01', '25');
INSERT INTO test.people VALUES('7', 'payment', '2019-02-01', '26');
INSERT INTO test.people VALUES('8', 'payment', '2019-04-01', '27');
INSERT INTO test.people VALUES('9', 'payment', '2019-05-01', '28');
INSERT INTO test.people VALUES('10', 'solution', '2019-08-01', '29');
INSERT INTO test.people VALUES('11', 'solution', '2019-08-01', '30');
INSERT INTO test.people VALUES('12', 'solution', '2019-09-01', '31');
INSERT INTO test.people VALUES('13', 'ML', '2019-03-01', '32');
INSERT INTO test.people VALUES('14', 'ML', '2019-12-01', '33');
INSERT INTO test.people VALUES('15', 'ML', '2019-03-01', '34');
INSERT INTO test.people VALUES('16', 'ML', '2019-02-01', '35');


SELECT
    ROW_NUMBER() OVER (ORDER BY age ASC)
FROM test.people 
Order BY id;


DROP TABLE test.people;
