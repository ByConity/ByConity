CREATE DATABASE IF NOT EXISTS test;
use test;

DROP TABLE IF EXISTS test.students;
DROP TABLE IF EXISTS test.students_local;
CREATE TABLE test.students_local (id Int, major String, age Int) ENGINE = MergeTree() PRIMARY KEY id order by id;

INSERT INTO test.students_local VALUES('1', 'Mathematics', '20');
INSERT INTO test.students_local VALUES('2', 'Mathematics', '21');
INSERT INTO test.students_local VALUES('3', 'Mathematics', '29');
INSERT INTO test.students_local VALUES('4', 'Mathematics', '23');
INSERT INTO test.students_local VALUES('5', 'Mathematics', '22');
INSERT INTO test.students_local VALUES('6', 'History', '20');
INSERT INTO test.students_local VALUES('7', 'History', '20');
INSERT INTO test.students_local VALUES('8', 'History', '21');
INSERT INTO test.students_local VALUES('9', 'History', '23');
INSERT INTO test.students_local VALUES('10', 'Civil engineering', '22');
INSERT INTO test.students_local VALUES('11', 'Civil engineering', '24');
INSERT INTO test.students_local VALUES('12', 'Civil engineering', '25');
INSERT INTO test.students_local VALUES('13', 'Computer', '21');
INSERT INTO test.students_local VALUES('14', 'Computer', '22');
INSERT INTO test.students_local VALUES('15', 'Computer', '24');
INSERT INTO test.students_local VALUES('16', 'Computer', '25');

CREATE TABLE students AS students_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), students_local);

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
DROP TABLE students_local;
