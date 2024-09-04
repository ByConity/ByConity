CREATE DATABASE IF NOT EXISTS test;
use test;

DROP TABLE IF EXISTS test.students;
CREATE TABLE test.students (id Int, major String, age Int) ENGINE = CnchMergeTree() PRIMARY KEY id order by id;

INSERT INTO test.students VALUES('1', 'Mathematics', '20');
INSERT INTO test.students VALUES('2', 'Mathematics', '21');
INSERT INTO test.students VALUES('3', 'Mathematics', '29');
INSERT INTO test.students VALUES('4', 'Mathematics', '23');
INSERT INTO test.students VALUES('5', 'Mathematics', '22');
INSERT INTO test.students VALUES('6', 'History', '20');
INSERT INTO test.students VALUES('7', 'History', '20');
INSERT INTO test.students VALUES('8', 'History', '21');
INSERT INTO test.students VALUES('9', 'History', '23');
INSERT INTO test.students VALUES('10', 'Civil engineering', '22');
INSERT INTO test.students VALUES('11', 'Civil engineering', '24');
INSERT INTO test.students VALUES('12', 'Civil engineering', '25');
INSERT INTO test.students VALUES('13', 'Computer', '21');
INSERT INTO test.students VALUES('14', 'Computer', '22');
INSERT INTO test.students VALUES('15', 'Computer', '24');
INSERT INTO test.students VALUES('16', 'Computer', '25');

set enable_optimizer=1;
set send_logs_level='warning';
set enable_windows_reorder=1;

SELECT '----test reorder adjacent windows';
explain SELECT
            id,
            major,
            AVG(age) OVER (PARTITION BY major ORDER BY id),
            count(major) OVER (PARTITION BY major ORDER BY id),
            rank() OVER (PARTITION BY major,id ORDER BY age)
        FROM students
        Order by major, id;


SELECT '----test no swap to adjacent windows';
set enable_windows_reorder=0;
explain SELECT
            id,
            major,
            AVG(age) OVER (PARTITION BY major ORDER BY id),
            count(major) OVER (PARTITION BY major ORDER BY id),
            rank() OVER (PARTITION BY major,id ORDER BY age)
        FROM students
        Order by major, id;

DROP TABLE students;
