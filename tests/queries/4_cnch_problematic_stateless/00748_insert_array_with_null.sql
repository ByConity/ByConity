DROP TABLE IF EXISTS test.arraytest;

CREATE TABLE test.arraytest ( created_date Date DEFAULT toDate(created_at), created_at DateTime DEFAULT now(), strings Array(String) DEFAULT emptyArrayString()) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(created_date) SAMPLE BY cityHash64(created_at) ORDER BY (created_date, cityHash64(created_at)) SETTINGS index_granularity=8192;

INSERT INTO test.arraytest (created_at, strings) VALUES (now(), ['aaaaa', 'bbbbb', 'ccccc']);
INSERT INTO test.arraytest (created_at, strings) VALUES (now(), ['aaaaa', 'bbbbb', null]); -- { clientError 53 }

SELECT strings from test.arraytest;

DROP TABLE IF EXISTS test.arraytest;

