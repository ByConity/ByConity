DROP TABLE IF EXISTS test_bitmap_index_non_filter;
set enable_ab_index_optimization = 1;

CREATE TABLE IF NOT EXISTS test_bitmap_index_non_filter (`date` Date, `id` Int32, `int_vid` Array(Int32) BitmapIndex, `int_vid2` Array(Int32) BitmapIndex, `metrics` Int32) 
ENGINE = CnchMergeTree PARTITION BY date ORDER BY id settings enable_build_ab_index = 1;

INSERT INTO test_bitmap_index_non_filter VALUES ('2020-01-01', 1, [1, 2, 3], [10,11,12], 1);
INSERT INTO test_bitmap_index_non_filter VALUES ('2020-01-01', 2, [2, 3, 4], [11, 12, 13], 2);
INSERT INTO test_bitmap_index_non_filter VALUES ('2020-01-03', 3, [4, 5, 6, 3], [12, 13, 15], 3);
INSERT INTO test_bitmap_index_non_filter VALUES ('2020-01-02', 4, [7, 8, 9], [15, 12, 10], 5);
INSERT INTO test_bitmap_index_non_filter VALUES ('2020-01-02', 5, [7, 10, 11, 12], [14, 13, 12], 7);

SELECT 'array set check non filter';

SELECT arraySetCheck(int_vid, 3) FROM test_bitmap_index_non_filter ORDER BY id; -- 1, 1, 1, 0, 0
SELECT arraySetCheck(int_vid, 4) FROM test_bitmap_index_non_filter ORDER BY id; -- 0, 1, 1, 0, 0
SELECT multiIf(arraySetCheck(int_vid, 1), 1, arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 6), 6, arraySetCheck(int_vid, 8), 8, arraySetCheck(int_vid, 10), 10, 0) FROM test_bitmap_index_non_filter  ORDER BY id ASC; -- 1 2 6 8 10
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, id FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, 2) OR arraySetCheck(int_vid, 7) ORDER BY id ASC; -- 2 1/2 2/7 4/7 5
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, 2) OR arraySetCheck(int_vid, 7) GROUP BY multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) ORDER BY _vid ASC; -- 2 3 / 7 12
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, date, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, 2) OR arraySetCheck(int_vid, 7) GROUP BY multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0), date ORDER BY _vid, date ASC; -- 2 2020-01-01 3 / 7 2020-01-02 12
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, date, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE (arraySetCheck(int_vid, 2) OR arraySetCheck(int_vid, 7)) AND date='2020-01-02'  GROUP BY multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0), date ORDER BY _vid, date ASC; -- 7 2020-01-02 12
SELECT multiIf(arraySetCheck(int_vid, 3), 3, arraySetCheck(int_vid, 7), 7, 0) AS _vid, date, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE (arraySetCheck(int_vid, 3) AND (date = '2020-01-03')) OR arraySetCheck(int_vid, 7) GROUP BY multiIf(arraySetCheck(int_vid, 3), 3, arraySetCheck(int_vid, 7), 7, 0), date ORDER BY _vid ASC, date ASC; // 3 2020-01-03 3 / 7 2020-01-02 12;
-- SELECT multiIf(arraySetCheck(int_vid, 2, int_vid, 3), 23, arraySetCheck(int_vid, 7, int_vid2, 12), 712, 0) AS _vid, sum(metrics) FROM test_bitmap_index_non_filter WHERE arraySetCheck(int_vid, 2, int_vid, 3) OR arraySetCheck(int_vid, 7, int_vid2, 12) GROUP BY multiIf(arraySetCheck(int_vid, 2, int_vid, 3), 23, arraySetCheck(int_vid, 7, int_vid2, 12), 712, 0) order by _vid; // 23 3 / 712 12
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, (2, 7)) GROUP BY multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) ORDER BY _vid ASC; -- 2 3 / 7 12
SELECT multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) AS _vid, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, (2, 8)) GROUP BY multiIf(arraySetCheck(int_vid, 2), 2, arraySetCheck(int_vid, 7), 7, 0) ORDER BY _vid ASC;
SELECT if(arraySetCheck(int_vid, 2), 2, 0) AS _vid, SUM(metrics) FROM test_bitmap_index_non_filter  WHERE arraySetCheck(int_vid, 3) GROUP BY if(arraySetCheck(int_vid, 2), 2, 0) ORDER BY _vid ASC;



DROP TABLE IF EXISTS test_bitmap_index_non_filter;
