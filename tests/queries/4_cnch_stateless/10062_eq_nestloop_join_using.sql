DROP TABLE IF EXISTS nlj_lhs;
DROP TABLE IF EXISTS nlj_rhs;

CREATE TABLE nlj_lhs
(
    key   int,
    value Nullable(UInt8)
) ENGINE = CnchMergeTree order by tuple();

CREATE TABLE nlj_rhs
(
    key   int,
    value Nullable(UInt8)
) ENGINE = CnchMergeTree order by tuple();

INSERT INTO nlj_lhs VALUES (1, 1), (2, 3), (3, NULL);
INSERT INTO nlj_rhs VALUES (1, 1), (2, NULL), (3, 2);

SELECT key, value FROM nlj_lhs lhs JOIN nlj_rhs rhs USING (key, value) settings join_algorithm = 'nested_loop';

SELECT key, value FROM nlj_lhs lhs JOIN nlj_rhs rhs ON rhs.key=lhs.value settings join_algorithm = 'nested_loop';

SELECT key, value FROM nlj_lhs lhs JOIN nlj_rhs rhs USING (key, value) settings join_algorithm = 'nested_loop';

SELECT key, value FROM nlj_lhs lhs JOIN nlj_rhs rhs ON rhs.key=lhs.value settings join_algorithm = 'nested_loop';

DROP TABLE IF EXISTS nlj_lhs;
DROP TABLE IF EXISTS nlj_rhs;
