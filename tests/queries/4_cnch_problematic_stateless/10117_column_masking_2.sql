SET send_logs_level = 'none';

USE test;

DROP TABLE IF EXISTS test.col_masking;

DROP MASKING POLICY IF EXISTS credit_card_mask_test_1;
DROP MASKING POLICY IF EXISTS credit_card_mask_test_2;

CREATE MASKING POLICY credit_card_mask_test_1 AS (val Int32) -> CASE WHEN 123 IN (123, 345) THEN val % 100 ELSE val END;
CREATE MASKING POLICY credit_card_mask_test_2 AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE val END;

CREATE TABLE test.col_masking (
    `name` String,
    `age` Int64,
    `credit_card_num` Int32,
    `credit_card_str` String,
    `credit_card_num_plus_1` ALIAS `credit_card_num` + 1,
    `credit_card_str_prepend_a` ALIAS 'a' || `credit_card_str`
) ENGINE = CnchMergeTree() PARTITION BY `name` PRIMARY KEY `name` ORDER BY `name`;

INSERT INTO test.col_masking VALUES ('Jane', 18, 12345678, '12345678');
INSERT INTO test.col_masking VALUES ('Bob', 24, 87655678, '87655678');

SELECT `credit_card_num`, `credit_card_num_plus_1`, `credit_card_str`, `credit_card_str_prepend_a` FROM test.col_masking ORDER BY `name`;

ALTER TABLE test.col_masking MODIFY COLUMN credit_card_num SET MASKING POLICY credit_card_mask_test_1;
ALTER TABLE test.col_masking MODIFY COLUMN credit_card_str SET MASKING POLICY credit_card_mask_test_2;

-- alias columns are calculated by the original values
SELECT `credit_card_num`, `credit_card_num_plus_1`, `credit_card_str`, `credit_card_str_prepend_a` FROM test.col_masking ORDER BY `name`;

-- unable to mask for alias columns
ALTER TABLE test.col_masking MODIFY COLUMN credit_card_str_prepend_a SET MASKING POLICY credit_card_mask_test_2; -- { serverError 16 }

-- clean up
ALTER TABLE test.col_masking MODIFY COLUMN credit_card_num UNSET MASKING POLICY;
ALTER TABLE test.col_masking MODIFY COLUMN credit_card_str UNSET MASKING POLICY;

DROP MASKING POLICY IF EXISTS credit_card_mask_test_1;
DROP MASKING POLICY IF EXISTS credit_card_mask_test_2;

DROP TABLE IF EXISTS test.col_masking;
