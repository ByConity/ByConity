DROP TABLE IF EXISTS modify_column_after_fastdelete;

CREATE TABLE modify_column_after_fastdelete (id UInt64, s String) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO modify_column_after_fastdelete SELECT number, toString(number) FROM system.numbers limit 10;

set mutations_sync = 1;

ALTER TABLE modify_column_after_fastdelete fastdelete where id in (4,5);

SELECT 'after fastdelete';
SELECT count(1), sum(id), sum(toUInt64(s)) FROM modify_column_after_fastdelete;

ALTER TABLE modify_column_after_fastdelete MODIFY COLUMN s UInt64;

SELECT 'after modify column';
DESC modify_column_after_fastdelete;

SELECT count(1), sum(id), sum(s) FROM modify_column_after_fastdelete;

DROP TABLE modify_column_after_fastdelete;
