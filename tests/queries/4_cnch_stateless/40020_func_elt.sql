SELECT ELT(2, 'Aa', 'Bb', 'Cc');
SELECT ELT(4, 'Aa', 'Bb', 'Cc', 2000);
SELECT ELT(4, 'Aa', 'Bb', 'Cc', number) FROM (SELECT * FROM numbers(10));
SELECT ELT(4, 'Aa', 'Bb', 'Cc', 1000) FROM (SELECT * FROM numbers(10));
SELECT ELT(5, 'Aa', 'Bb', 'Cc', NULL);
SELECT ELT(NULL, 'Aa', 'Bb', 'Cc', NULL);
SELECT ELT(1, 'Aa', 'Bb', 'Cc', NULL);
SELECT ELT(-1, 'Aa', 'Bb', 'Cc', NULL);
SELECT ELT(4, 'Aa', 'Bb', 'Cc', (SELECT toDateTime('2023-01-13 17:23:28', 0, 'UTC')));
SELECT ELT(4, 'Aa', 'Bb', 'Cc', (SELECT(CAST(12.999 AS DECIMAL(7,1)))));

drop table if exists test_elt_tbl;
CREATE TABLE test_elt_tbl (
  id Int64,
  idx Int64,
  str1 String,
  str2 String,
  str3 Nullable(String)
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test_elt_tbl (id, idx, str1, str2, str3) VALUES (0, 1, 'hello', 'world', 'foo'), (1, 2, 'hello', 'nice', 'world'), (2, 3, 'nice', 'world', 'world'), (3, 1, '', 'foo', 'bar'), (4, 2, 'hello', 'nice', 'world'), (5, 0, 'hello', 'world', NULL), (6, NULL, 'hello', 'world', NULL), (7, -1, 'hello', 'world', NULL);
SELECT id, elt(idx, str1, str2, str3) FROM test_elt_tbl order by id;
drop table if exists test_elt_tbl;
