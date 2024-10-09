SET transform_null_in = 0;

select nullIn(null, tuple(1, 2, null));
select in(null, tuple(1, 2, null));
select nullIn(null, null);
select in(null, null);
select notNullIn(null, tuple(1, 2, null));
select notIn(null, tuple(1, 2, null));
select notNullIn(null, null);
select notIn(null, null);

DROP TABLE IF EXISTS null_in;
CREATE TABLE null_in (dt DateTime, idx int, i Nullable(int), s Nullable(String)) ENGINE = CnchMergeTree() PARTITION BY dt ORDER BY idx;

INSERT INTO null_in VALUES (1, 1, 1, '1') (2, 2, NULL, NULL) (3, 3, 3, '3') (4, 4, NULL, NULL) (5, 5, 5, '5');


SELECT count() == 4 FROM null_in WHERE nullIn(i, tuple(1, 3, NULL));
SELECT count() == 2 FROM null_in WHERE nullIn(i, range(4));
SELECT count() == 4 FROM null_in WHERE nullIn(s, tuple('1', '3', NULL));
SELECT count() == 4 FROM null_in WHERE globalNullIn(i, tuple(1, 3, NULL));
SELECT count() == 4 FROM null_in WHERE globalNullIn(s, tuple('1', '3', NULL));

SELECT count() == 4 FROM null_in WHERE notNullIn(i, tuple(1, 3, NULL));
SELECT count() == 2 FROM null_in WHERE notNullIn(i, range(4));
SELECT count() == 4 FROM null_in WHERE notNullIn(s, tuple('1', '3', NULL));
SELECT count() == 4 FROM null_in WHERE globalNotNullIn(i, tuple(1, 3, NULL));
SELECT count() == 4 FROM null_in WHERE globalNotNullIn(s, tuple('1', '3', NULL));

SELECT count() == 4 FROM null_in WHERE notNullIn(i, tuple(1, 3));
SELECT count() == 2 FROM null_in WHERE notNullIn(i, range(4));
SELECT count() == 4 FROM null_in WHERE notNullIn(s, tuple('1', '3'));
SELECT count() == 4 FROM null_in WHERE globalNotNullIn(i, tuple(1, 3));
SELECT count() == 4 FROM null_in WHERE globalNotNullIn(s, tuple('1', '3'));

DROP TABLE IF EXISTS null_in;
