SELECT sleep(2)
INTO OUTFILE 'hdfs:///user/clickhouse/outfile_failed_select.parquet' FORMAT Parquet 
SETTINGS max_execution_time=1, enable_optimizer=1, outfile_in_server_with_tcp=1, overwrite_current_file=1; -- { serverError 159 }

SELECT 'Hello, World! From client.'
