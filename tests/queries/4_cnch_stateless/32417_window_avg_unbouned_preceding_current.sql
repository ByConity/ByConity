DROP DATABASE IF EXISTS test;


DROP TABLE IF EXISTS avg_unbounded_preceding_current;
CREATE TABLE avg_unbounded_preceding_current (id Int, department String, onboard_date String, age Int) ENGINE = CnchMergeTree() PRIMARY KEY id order by id;

INSERT INTO avg_unbounded_preceding_current VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO avg_unbounded_preceding_current VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO avg_unbounded_preceding_current VALUES('3', 'data', '2019-02-01', '29');
INSERT INTO avg_unbounded_preceding_current VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO avg_unbounded_preceding_current VALUES('5', 'data', '2019-04-01', '22');
INSERT INTO avg_unbounded_preceding_current VALUES('6', 'payment', '2019-01-01', '20');
INSERT INTO avg_unbounded_preceding_current VALUES('7', 'payment', '2019-02-01', '20');
INSERT INTO avg_unbounded_preceding_current VALUES('8', 'payment', '2019-04-01', '21');
INSERT INTO avg_unbounded_preceding_current VALUES('9', 'payment', '2019-05-01', '23');
INSERT INTO avg_unbounded_preceding_current VALUES('10', 'solution', '2019-08-01', '22');
INSERT INTO avg_unbounded_preceding_current VALUES('11', 'solution', '2019-08-01', '24');
INSERT INTO avg_unbounded_preceding_current VALUES('12', 'solution', '2019-09-01', '25');
INSERT INTO avg_unbounded_preceding_current VALUES('13', 'ML', '2019-03-01', '21');
INSERT INTO avg_unbounded_preceding_current VALUES('14', 'ML', '2019-12-01', '22');
INSERT INTO avg_unbounded_preceding_current VALUES('15', 'ML', '2019-03-01', '24');
INSERT INTO avg_unbounded_preceding_current VALUES('16', 'ML', '2019-02-01', '25');


SELECT 
    id, 
    department, 
    AVG(age) OVER (PARTITION BY department ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
FROM avg_unbounded_preceding_current 
Order by department, id;

DROP TABLE avg_unbounded_preceding_current;
