DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

DROP TABLE IF EXISTS test.max_avg_parition_by;
CREATE TABLE test.max_avg_parition_by (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() PRIMARY KEY id order by id;

INSERT INTO test.max_avg_parition_by VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO test.max_avg_parition_by VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO test.max_avg_parition_by VALUES('3', 'data', '2019-02-01', '29');
INSERT INTO test.max_avg_parition_by VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO test.max_avg_parition_by VALUES('5', 'data', '2019-04-01', '22');
INSERT INTO test.max_avg_parition_by VALUES('6', 'payment', '2019-01-01', '20');
INSERT INTO test.max_avg_parition_by VALUES('7', 'payment', '2019-02-01', '20');
INSERT INTO test.max_avg_parition_by VALUES('8', 'payment', '2019-04-01', '21');
INSERT INTO test.max_avg_parition_by VALUES('9', 'payment', '2019-05-01', '23');
INSERT INTO test.max_avg_parition_by VALUES('10', 'solution', '2019-08-01', '22');
INSERT INTO test.max_avg_parition_by VALUES('11', 'solution', '2019-08-01', '24');
INSERT INTO test.max_avg_parition_by VALUES('12', 'solution', '2019-09-01', '25');
INSERT INTO test.max_avg_parition_by VALUES('13', 'ML', '2019-03-01', '21');
INSERT INTO test.max_avg_parition_by VALUES('14', 'ML', '2019-12-01', '22');
INSERT INTO test.max_avg_parition_by VALUES('15', 'ML', '2019-03-01', '24');
INSERT INTO test.max_avg_parition_by VALUES('16', 'ML', '2019-02-01', '25');



SELECT 
    id, 
    department,
    MAX(AVG(age))  OVER (PARTITION BY department)
FROM test.max_avg_parition_by 
GROUP BY id, department 
ORDER BY department, id;


DROP TABLE test.max_avg_parition_by;
