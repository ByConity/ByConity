
CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.window_percent_rank;
CREATE TABLE test.window_percent_rank (id Int, department String, onboard_date String, age Int) ENGINE = Memory;

INSERT INTO test.window_percent_rank VALUES('1', 'data', '2019-01-01', '20');
INSERT INTO test.window_percent_rank VALUES('2', 'data', '2019-03-01', '21');
INSERT INTO test.window_percent_rank VALUES('3', 'data', '2019-02-01', '29');
INSERT INTO test.window_percent_rank VALUES('4', 'data', '2019-03-01', '23');
INSERT INTO test.window_percent_rank VALUES('5', 'data', '2019-04-01', '22');
INSERT INTO test.window_percent_rank VALUES('6', 'payment', '2019-01-01', '20');
INSERT INTO test.window_percent_rank VALUES('7', 'payment', '2019-02-01', '20');
INSERT INTO test.window_percent_rank VALUES('8', 'payment', '2019-04-01', '21');
INSERT INTO test.window_percent_rank VALUES('9', 'payment', '2019-05-01', '23');
INSERT INTO test.window_percent_rank VALUES('10', 'solution', '2019-08-01', '22');
INSERT INTO test.window_percent_rank VALUES('11', 'solution', '2019-08-01', '24');
INSERT INTO test.window_percent_rank VALUES('12', 'solution', '2019-09-01', '25');
INSERT INTO test.window_percent_rank VALUES('13', 'ML', '2019-03-01', '21');
INSERT INTO test.window_percent_rank VALUES('14', 'ML', '2019-12-01', '22');
INSERT INTO test.window_percent_rank VALUES('15', 'ML', '2019-03-01', '24');
INSERT INTO test.window_percent_rank VALUES('16', 'ML', '2019-02-01', '25');


SELECT 
    id,
    sum(age) over (PARTITION BY department ORDER by id rows between 1 preceding and 1 following),
    Avg(age) OVER (PARTITION BY department ORDER by id rows between 1 preceding and 1 following),
    rank() OVER (PARTITION BY department ORDER BY id),
    percent_rank() OVER (PARTITION BY department ORDER BY id) 
FROM test.window_percent_rank 
Order by department, id
settings allow_experimental_window_functions = true;

SELECT 
    id,
    sum(age) over (PARTITION BY department ORDER by id rows between 1 preceding and 1 following),
    Avg(age) OVER (PARTITION BY department ORDER by id rows between 1 preceding and 1 following),
    rank() OVER (PARTITION BY department ORDER BY id),
    percent_rank() OVER (PARTITION BY department ORDER BY id) 
FROM test.window_percent_rank 
Order by department, id
settings max_block_size = 2, allow_experimental_window_functions = true;



DROP TABLE test.window_percent_rank;
