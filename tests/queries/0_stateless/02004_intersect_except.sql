-- General tests
SELECT 'ByteHouse' INTERSECT SELECT 'ByteHouse';
SELECT 'ByteHouse' EXCEPT SELECT 'ByteHouse';

SELECT '-- INTERSECT --';
SELECT number FROM numbers(10) INTERSECT SELECT number FROM numbers(5);
SELECT toString(number) FROM numbers(10) INTERSECT SELECT toString(number) FROM numbers(5);
SELECT 1 FROM numbers(10) INTERSECT SELECT 1 FROM numbers(10);
SELECT '1' FROM numbers(10) INTERSECT SELECT '1' FROM numbers(10);
SELECT toString(1) FROM numbers(10) INTERSECT SELECT toString(1) FROM numbers(10);

SELECT '-- INTERSECT ALL --';
SELECT number FROM numbers(10) INTERSECT ALL SELECT number FROM numbers(5);
SELECT toString(number) FROM numbers(10) INTERSECT ALL SELECT toString(number) FROM numbers(5);
SELECT 1 FROM numbers(10) INTERSECT ALL SELECT 1 FROM numbers(10);
SELECT '1' FROM numbers(10) INTERSECT ALL SELECT '1' FROM numbers(10);
SELECT toString(1) FROM numbers(10) INTERSECT ALL SELECT toString(1) FROM numbers(10);

SELECT '-- EXCEPT --';
SELECT number FROM numbers(10) EXCEPT SELECT number FROM numbers(5);
SELECT toString(number) FROM numbers(10) EXCEPT SELECT toString(number) FROM numbers(5);
SELECT 1 FROM numbers(10) EXCEPT SELECT 2 FROM numbers(10);
SELECT '1' FROM numbers(10) EXCEPT SELECT '2' FROM numbers(10);
SELECT toString(1) FROM numbers(10) EXCEPT SELECT toString(2) FROM numbers(10);

SELECT '-- EXCEPT ALL --';
SELECT number FROM numbers(10) EXCEPT ALL SELECT number FROM numbers(5);
SELECT toString(number) FROM numbers(10) EXCEPT ALL SELECT toString(number) FROM numbers(5);
SELECT 1 FROM numbers(10) EXCEPT ALL SELECT 2 FROM numbers(10);
SELECT '1' FROM numbers(10) EXCEPT ALL SELECT '2' FROM numbers(10);
SELECT toString(1) FROM numbers(10) EXCEPT ALL SELECT toString(2) FROM numbers(10);
