SELECT LENGTH('text');
SELECT LENGTH('textaa');
SELECT LENGTH(NULL);
SELECT LENGTH('ÃÃÃ');
SELECT LENGTH('ssŜ');
SELECT LENGTH('a');
SELECT LENGTH(toFixedString('text', 4));
SELECT LENGTH(toFixedString('aa', 2));
SELECT OCTET_LENGTH('text');
SELECT OCTET_LENGTH('textaa');
SELECT OCTET_LENGTH(NULL);
SELECT OCTET_LENGTH('ÃÃÃ');
SELECT OCTET_LENGTH('ssŜ');
SELECT OCTET_LENGTH('a');
SELECT OCTET_LENGTH(toFixedString('text', 4));
SELECT OCTET_LENGTH(toFixedString('aa', 2));

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS length;
CREATE TABLE length
(
    id Int64,
    a Nullable(String),
    b String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO length (id, a, b) VALUES (0, 'example', 'example'), (1, 'apple', 'banana'), (2, 'zebra', 'ant'), (3, '', ''), (4, '', 'nonempty'), (5, 'nonempty', ''), (6, 'CaseTest', 'Chance'), (7, 'EmPtYsTrInG', 'empty?'), (8, 'test123', 'test@#'), (9, 'long_string_1', 'long_string_2'), (10, NULL, 'long_string_2');
INSERT INTO length (id, a, b) VALUES (11, NULL, 'exam'), (12, 'apple', 'bana'), (13, 'zebra', 'ant0'), (14, '888', '888'), (15, '', 'none'), (16, 'test123', 'test'), (17, 'long_string_1', 'long');

SELECT id, length(a) FROM length ORDER BY id;
SELECT id, octet_length(a) FROM length ORDER BY id;
SELECT id, length(b) FROM length ORDER BY id;
SELECT id, octet_length(b) FROM length ORDER BY id;

DROP TABLE IF EXISTS length;