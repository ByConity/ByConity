CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.ppl2;
CREATE TABLE test.ppl2 (id Int, department String, onboard_date String, age Int) ENGINE = Memory;

INSERT INTO test.ppl2 VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO test.ppl2 VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO test.ppl2 VALUES('3', 'data', '2019-02-01', '29');
INSERT INTO test.ppl2 VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO test.ppl2 VALUES('5', 'data', '2019-04-01', '22');
INSERT INTO test.ppl2 VALUES('6', 'payment', '2019-01-01', '20');
INSERT INTO test.ppl2 VALUES('7', 'payment', '2019-02-01', '20');
INSERT INTO test.ppl2 VALUES('8', 'payment', '2019-04-01', '21');
INSERT INTO test.ppl2 VALUES('9', 'payment', '2019-05-01', '23');
INSERT INTO test.ppl2 VALUES('10', 'solution', '2019-08-01', '22');
INSERT INTO test.ppl2 VALUES('11', 'solution', '2019-08-01', '24');
INSERT INTO test.ppl2 VALUES('12', 'solution', '2019-09-01', '25');
INSERT INTO test.ppl2 VALUES('13', 'ML', '2019-03-01', '21');
INSERT INTO test.ppl2 VALUES('14', 'ML', '2019-12-01', '22');
INSERT INTO test.ppl2 VALUES('15', 'ML', '2019-03-01', '24');
INSERT INTO test.ppl2 VALUES('16', 'ML', '2019-02-01', '25');


SELECT 
    id,
    AVG(age) OVER (PARTITION BY department ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) 
FROM test.ppl2 
Order by department, id;



DROP TABLE test.ppl2;
