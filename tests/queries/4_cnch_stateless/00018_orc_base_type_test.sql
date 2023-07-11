DROP TABLE IF EXISTS orc_base_test;

CREATE TABLE orc_base_test
(
    tiny Tinyint, 
    small Smallint,
    int_32 int, 
    big_int Bigint, 
    str String, 
    fix_str FixedString(5), 
    float1 FLOAT, 
    double1 DOUBLE,
    date String,
    app_name String
)
ENGINE = CnchHive(`data.olap.cnch_hms.service.lf`, `cnch_hive_external_table`, `orc_base_type`)
PARTITION BY (date, app_name);

select * from orc_base_test where date = '20221011' order by tiny;

DROP TABLE IF EXISTS orc_base_test;
