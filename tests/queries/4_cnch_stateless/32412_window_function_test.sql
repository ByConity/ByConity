CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS people;
CREATE TABLE people (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() PRIMARY KEY id order by id;

INSERT INTO people VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO people VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO people VALUES('3', 'data', '2019-02-01', '29');
INSERT INTO people VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO people VALUES('5', 'data', '2019-04-01', '22');
INSERT INTO people VALUES('6', 'payment', '2019-01-01', '20');
INSERT INTO people VALUES('7', 'payment', '2019-02-01', '20');
INSERT INTO people VALUES('8', 'payment', '2019-04-01', '21');
INSERT INTO people VALUES('9', 'payment', '2019-05-01', '23');
INSERT INTO people VALUES('10', 'solution', '2019-08-01', '22');
INSERT INTO people VALUES('11', 'solution', '2019-08-01', '24');
INSERT INTO people VALUES('12', 'solution', '2019-09-01', '25');
INSERT INTO people VALUES('13', 'ML', '2019-03-01', '21');
INSERT INTO people VALUES('14', 'ML', '2019-12-01', '22');
INSERT INTO people VALUES('15', 'ML', '2019-03-01', '24');
INSERT INTO people VALUES('16', 'ML', '2019-02-01', '25');



SELECT 
    id, 
    department,
    MAX(AVG(age))  OVER (PARTITION BY department)
FROM people 
GROUP BY id, department 
ORDER BY department, id;


DROP TABLE people;
