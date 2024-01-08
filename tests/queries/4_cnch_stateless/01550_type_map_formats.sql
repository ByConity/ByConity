SET allow_experimental_map_type = 1;
SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS map_formats;
CREATE TABLE map_formats (m Map(String, UInt32) KV, m1 Map(String, Date) KV, m2 Map(String, Array(UInt32)) KV) ENGINE = CnchMergeTree order by tuple();

INSERT INTO map_formats VALUES(map('k1', 1, 'k2', 2, 'k3', 3), map('k1', toDate('2020-05-05')), map('k1', [], 'k2', [7, 8]));
INSERT INTO map_formats VALUES(map('k1', 10, 'k3', 30), map('k2', toDate('2020-06-06')), {});

-- arrayElement for DataTypeByteMap is currently not support in optimizer process
SELECT 'JSON';
-- SELECT * FROM map_formats ORDER BY m['k1'] FORMAT JSON;
SELECT * FROM map_formats ORDER BY m{'k1'} FORMAT JSON;
SELECT 'JSONEachRow';
-- SELECT * FROM map_formats ORDER BY m['k1'] FORMAT JSONEachRow;
SELECT * FROM map_formats ORDER BY m{'k1'} FORMAT JSONEachRow;
SELECT 'CSV';
-- SELECT * FROM map_formats ORDER BY m['k1'] FORMAT CSV;
SELECT * FROM map_formats ORDER BY m{'k1'} FORMAT CSV;
SELECT 'TSV';
-- SELECT * FROM map_formats ORDER BY m['k1'] FORMAT TSV;
SELECT * FROM map_formats ORDER BY m{'k1'} FORMAT TSV;
SELECT 'TSKV';
-- SELECT * FROM map_formats ORDER BY m['k1'] FORMAT TSKV;
SELECT * FROM map_formats ORDER BY m{'k1'} FORMAT TSKV;

DROP TABLE map_formats;

CREATE TABLE map_formats (m Map(String, UInt32) KV, m1 Map(String, Date) KV, m2 Map(String, Array(UInt32)) KV) ENGINE = CnchMergeTree order by tuple();

SELECT 'TEST INSERT NULL MAP';
SET input_format_null_as_default = false;
SET input_format_parse_null_map_as_empty = false;
INSERT INTO map_formats VALUES(NULL, NULL, {}); -- { clientError 53 }
SET input_format_null_as_default = true;
INSERT INTO map_formats VALUES(NULL, NULL, {});
SELECT * FROM map_formats;

DROP TABLE map_formats;

CREATE TABLE map_formats (m Map(String, UInt32) KV, m1 Map(String, Date) KV, m2 Map(String, Array(UInt32)) KV) ENGINE = CnchMergeTree order by tuple();

SELECT 'TEST INSERT NULL VALUE';
SET input_format_skip_null_map_value = false;
INSERT INTO map_formats VALUES({'k1': null, 'k3': 30}, {'k2': '2020-06-06'}, {}); -- { clientError 62 }
SET input_format_skip_null_map_value = true;
INSERT INTO map_formats VALUES({'k1': null, 'k3': 30}, {'k2': '2020-06-06'}, {});
SELECT * FROM map_formats;

DROP TABLE map_formats;
