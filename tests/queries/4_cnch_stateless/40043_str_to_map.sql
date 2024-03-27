SELECT str_to_map('ABC_ABC, AC_AC', ',', '_');
SELECT str_to_map('', 'a', 'b');
SELECT str_to_map('ab', 'b', 'a');
SELECT str_to_map('a=2&b=3', '&', '=');
SELECT str_to_map('a=&b=', '&', '=');
SELECT str_to_map(null, '&', '=');  -- { serverError 43 }

DROP TABLE IF EXISTS 40043_str_to_map;

CREATE TABLE 40043_str_to_map (a String, b String, c String, d String) ENGINE = CnchMergeTree ORDER BY a;
INSERT INTO 40043_str_to_map VALUES ('a', 'b', 'c', 'd');
INSERT INTO 40043_str_to_map VALUES ('a.b', 'b.c', 'c.d', 'd.f');

SELECT str_to_map(concat(a, '$', b, '-', c, '$', d), '-', '$') FROM 40043_str_to_map ORDER BY a;
DROP TABLE 40043_str_to_map;

CREATE TABLE 40043_str_to_map (n Int32, a Nullable(String)) ENGINE = CnchMergeTree ORDER BY n;
INSERT INTO 40043_str_to_map VALUES (0, 'a=2&b=3') (1, null);
SELECT str_to_map(a, '&', '=') FROM 40043_str_to_map ORDER BY n;

DROP TABLE 40043_str_to_map;
