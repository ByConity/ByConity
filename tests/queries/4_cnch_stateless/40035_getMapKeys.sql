SET enable_optimizer=1;
set enable_auto_query_forwarding=0;

DROP TABLE IF EXISTS aeolus_data_table_177956_prod;

CREATE TABLE aeolus_data_table_177956_prod
(
    `row_id_kmtq3k` Int64,
    `p_date` Date,
    `user_id` Nullable(Int64),
    `mission_id` Nullable(Int64),
    `name` Nullable(String),
    `group_vals` Map(String, String),
    `stat_vals` Map(String, String),
    `location` Nullable(String),
    `bhv_vals` Map(String, Int32)
)
ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (row_id_kmtq3k, intHash64(row_id_kmtq3k));

select getMapKeys(currentDatabase(), 'aeolus_data_table_177956_prod', 'group_vals', '.*2023.*2.*7.*', 60) as a;
select arrayJoin(getMapKeys(currentDatabase(), 'aeolus_data_table_177956_prod', 'group_vals', '.*2023.*2.*7.*', 60)) as a;

-- getMapKeys should be executed in server side
explain select getMapKeys(currentDatabase(), 'aeolus_data_table_177956_prod', 'group_vals', '.*2023.*2.*7.*', 60) as a;
explain select arrayJoin(getMapKeys(currentDatabase(), 'aeolus_data_table_177956_prod', 'group_vals', '.*2023.*2.*7.*', 60)) as a;
