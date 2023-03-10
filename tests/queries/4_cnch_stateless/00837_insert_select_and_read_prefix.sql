USE default;
DROP TABLE IF EXISTS file;
CREATE TABLE file (s String, n UInt32) ENGINE = File(CSVWithNames);
-- BTW, WithNames formats are totally unsuitable for more than a single INSERT
INSERT INTO file VALUES ('hello', 1), ('world', 2);

SELECT * FROM file order by s;
CREATE TEMPORARY TABLE file2 AS SELECT * FROM file;
SELECT * FROM file2 order by s;;

DROP TABLE file;
