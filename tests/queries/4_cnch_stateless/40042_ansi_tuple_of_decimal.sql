SET dialect_type='ANSI';

SELECT (1, 2.1, 3);
SELECT [1, 2.1, 3];
SELECT tupleHammingDistance((1, 2.1, 3), (3, 2, 1.2)) AS HammingDistance;
