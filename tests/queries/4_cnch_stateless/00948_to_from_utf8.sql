SELECT from_utf8(to_utf8('China')) as result;

SELECT from_utf8(to_utf8('こんにちは世界')) as result;

SELECT from_utf8(to_utf8('')) as result;

SELECT from_utf8('0xAbcG123') as result;

SELECT from_utf8('0x4368696E61') as result;

SELECT from_utf8('\\xC3\\x28') as result;

SELECT from_utf8('\\\\x43\\\\x68\\\\x69\\\\x6E\\\\x61') as result;

SELECT from_utf8(to_utf8('This is a long string with multiple words and some special characters like: @#$%^&*()-=_+[]{}|;:,.<>?/`~')) as result;

SELECT from_utf8(to_utf8('这是一个很长的字符串，其中包含多个单词和一些特殊字符，如：@＃$％^＆*()-=_+[]{}|;：，. <>？/`~')) as result;

DROP TABLE IF EXISTS utf8;
CREATE TABLE utf8
(
    a Nullable(String),
    b String
)
ENGINE = CnchMergeTree()
ORDER BY b;

INSERT INTO utf8 (a, b) VALUES ('my name is www', 'age:9999'), ('why, is, this, format, stupid', 'look:normal'), ('let-me-do-it', 'country:Singapore'), (NULL, 'country:China'), (NULL, 'country:us'), ('sing', 'city:tokyo'), ('gojira,is,king', 'city:tokyo'), ('gojira--are--king', 'city:tokyo');

SELECT from_utf8(to_utf8(a)), from_utf8(to_utf8(b)) FROM utf8 ORDER BY b;

DROP TABLE IF EXISTS utf8;