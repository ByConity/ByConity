use test;
DROP TABLE IF EXISTS dm_local_life_account_stats;
set enable_implicit_arg_type_convert=1;
CREATE TABLE test.dm_local_life_account_stats
(
    `p_date` Date,
    `account_id` Int64
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY (account_id, intHash64(account_id));
SELECT *
FROM
(
    SELECT
        p_date AS pdate
    FROM test.dm_local_life_account_stats
) AS tmp_05f371fe_59e5_4c33_8754_1707a52af5d1
LEFT JOIN
(
    SELECT
        p_date AS p_date
    FROM test.dm_local_life_account_stats
) AS tmp_3817da9e_e057_4aed_89a0_a2095ca20a1a ON (tmp_05f371fe_59e5_4c33_8754_1707a52af5d1.pdate = tmp_3817da9e_e057_4aed_89a0_a2095ca20a1a.p_date) AND (pdate = '2024-08-17');
DROP TABLE IF EXISTS dm_local_life_account_stats;

DROP TABLE IF EXISTS test.aeolus_data_table_2_2516793_prod;
CREATE TABLE test.aeolus_data_table_2_2516793_prod ( `row_id_kmtq3k` Int64, `p_date` Date) ENGINE = CnchMergeTree() PARTITION BY p_date ORDER BY (row_id_kmtq3k, intHash64(row_id_kmtq3k));

insert into  test.aeolus_data_table_2_2516793_prod values(1,'2024-09-01');
SELECT *
FROM test.aeolus_data_table_2_2516793_prod
WHERE p_date = (
    SELECT toString(max(p_date))
    FROM test.aeolus_data_table_2_2516793_prod
) format Null;