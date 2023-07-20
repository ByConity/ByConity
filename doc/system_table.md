ByConity System Tables
Following are the system tables available related to CNCH.
common column:
- database: the database name of the table
- table: the table name of the table
- uuid: the uuid value of the table

**cnch_databases**

```
┌─name─────────────┬─type─────┬
│ name             │ String   │
│ txn_id           │ UInt64   │
│ previous_version │ UInt64   │
│ commit_time      │ DateTime │
└──────────────────┴──────────┴
```
- name: name of the database
- txn_id: the last transaction id that applied to the database (create/rename).
- previous_version: the commit timestamp of the previous version of the database
- commit time: the last commit timestamp in human readable form.

**cnch_databases_history**

Contains information about the databases that have just been dropped. When a database is dropped, its information won't be cleaned immediately. It remains for a period of time (currently the default value is 3 days). While it is still remains, its information can be queried from this table
```
┌─name────────┬─type─────┬
│ name        │ String   │
│ delete_time │ DateTime │
└─────────────┴──────────┴
```

- name: database name
- delete_time: the time it got dropped

**cnch_tables**

Contains information about all cnch tables in the cluster
```
┌─name──────────────┬─type─────┬
│ database          │ String   │
│ name              │ String   │
│ uuid              │ UUID     │
│ vw_name           │ String   │
│ definition        │ String   │
│ txn_id            │ UInt64   │
│ previous_version  │ UInt64   │
│ current_version   │ UInt64   │
│ modification_time │ DateTime │
│ is_preallocated   │ UInt8    │
│ is_detached       │ UInt8    │
│ partition_key     │ String   │
│ sorting_key       │ String   │
│ primary_key       │ String   │
│ sampling_key      │ String   │
│ cluster_key       │ String   │
└───────────────────┴──────────┴
```
- vw_name: virtual warehouse name, contain vw name for preallocated table.
- definition: the CREATE query of the table
- txn_id: transaction id of the last action on the table(action include modify table schema, create table)
- previous_version: the previous value of commit timestamp of the table
- current_version: the commit timestamp of the last action on the table
- modification_time: commit timestamp in human readable form
- is_preallocated: whether or not the table is in preallocated mode
- is_detached: whether or not the table is currently detached
- partition_key: partition key of the CnchMergeTree engine
- sorting_key: sorting key of the CnchMergeTree engine
- primary_key: primary key of the CnchMergeTree engine
- sampling_key: sampling key of the CnchMergeTree engine
- cluster_key: cluster key for the bucket table

**cnch_table_info**

```
┌─name───────────────────┬─type─────┬
│ database               │ String   │
│ table                  │ String   │
│ last_modification_time │ DateTime │
│ cluster_status         │ UInt8    │
└────────────────────────┴──────────┴
```

- last_modification_time: timestamp when last data ingestion or removal happens to this table;
- cluster_status: for bucket table, if all the parts of the table have been clustered. Have no meaning for normal table.

**cnch_tables_history**

Contains information about the tables that have just been dropped. When a table is dropped, its information won't be cleaned immediately. It remains for a period of time (currently the default value is 3 days). While it is still remains, its information can be queried from this table
```
┌─name─────────────┬─type─────┬
│ database         │ String   │
│ name             │ String   │
│ uuid             │ UUID     │
│ definition       │ String   │
│ current_version  │ UInt64   │
│ previous_version │ UInt64   │
│ delete_time      │ DateTime │
└──────────────────┴──────────┴
```

- definition: the CREATE query of the table
- current_version: the transaction id of the drop action
- previous_version: the timestamp of the previous version of the table (not the commit time of the table before drop)
- delete_time: the timestamp of the drop action

**cnch_vw**

This usually don't contain data until preallocate mode is used with some query or table.
```
┌─name─┬─type───┬
│ name │ String │
└──────┴────────┴
```
- name: virtual warehouse name

**cnch_parts**

Select * from system.cnch_parts where database = 'abc' and table = 'dummy';
For parts, we need to atleast specify database and table name for each query
```
┌─name────────────────┬─type────┬─default_type─┬─default_expression─┬
│ partition           │ String  │              │                    │
│ name                │ String  │              │                    │
│ database            │ String  │              │                    │
│ table               │ String  │              │                    │
│ bytes_on_disk       │ UInt64  │              │                    │
│ rows_count          │ UInt64  │              │                    │
│ columns             │ String  │              │                    │
│ marks_count         │ UInt64  │              │                    │
│ ttl                 │ String  │              │                    │
│ commit_time         │ DateTime│              │                    │
│ columns_commit_time │ DateTime│              │                    │
│ previous_version    │ UInt64  │              │                    │
│ partition_id        │ String  │              │                    │
│ visible             │ UInt8   │              │                    │
│ bucket_number       │ Int64   │              │                    │
│ has_bitmap          │ Int8    │              │                    │
│ bytes               │ UInt64  │ ALIAS        │ bytes_on_disk      │
│ rows                │ UInt64  │ ALIAS        │ rows_count         │
│ active              │ UInt8   │ ALIAS        │ visible            │
└─────────────────────┴─────────┴──────────────┴────────────────────┴
```
- partition: the partition name where the part belongs to,  '2021-04-04'
- name: the name of the part,  20210404_424025941137162264_424025941137162264_0_424025938738282498
- bytes_on_disk: the number of bytes that the part takes on disk
- rows_count: number of the rows in the part
- columns: description of column in the part
- marks_count: the number of marks. To get the approximate number of rows in a data part, multiply marks by the index granularity (usually 8192)
- ttl: 2 values corresponding to ttl min and ttl max of the table
- commit_time: the commit timestamp in human readable form
- columns_commit_time: the time where the schema of the table corresponding with the part is committed.
- previous_version: (also called hint_mutation) commit time of the previous part in part chain. For the part that have not been mutated, the previous_version = 0.
- partition_id: the partition id where the part belongs to, 20210404
- visible: if the part is visible to the query with latest timestamp or not
- bucket_number: the bucket number of the part (it is -1 if the part is not bucketed) 
- has_bitmap: whether or not the part has bitmap

**cnch_preallocated_topology**

Can be used to get worker side topology for preallocated table
Select * from system.cnch_preallocated_topology where database='x' and table='y'
we need to atleast specify database and table name
```
┌─name───────────┬─type───┬
│ database       │ String │
│ table          │ String │
│ vw_name        │ String │
│ worker_address │ String │
└────────────────┴────────┴
```
- vw_name: the virtual warehouse that the worker belongs to
- worker_address: Worker's host ip address with TCP and RPC ports

**cnch_kafka_log**

A separated document about system table for Cnch Kafka is here
```
┌─name──────────┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────┬
│ event_type    │ Enum8('EMPTY' = 0, 'POLL' = 1, 'PARSE_ERROR' = 2, 'WRITE' = 3, 'EXCEPTION' = 4, 'EMPTY_MESSAGE' = 5, 'FILTER' = 6) │
│ event_date    │ Date                                                                                                               │
│ event_time    │ DateTime                                                                                                           │
│ duration_ms   │ UInt64                                                                                                             │
│ cnch_database │ String                                                                                                             │
│ cnch_table    │ String                                                                                                             │
│ database      │ String                                                                                                             │
│ table         │ String                                                                                                             │
│ consumer      │ String                                                                                                             │
│ metric        │ UInt64                                                                                                             │
│ bytes         │ UInt64                                                                                                             │
│ has_error     │ UInt8                                                                                                              │
│ exception     │ String                                                                                                             │
└───────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴
```

**cnch_kafka_tables**

A separated document about system table for Cnch Kafka is here
Contains information about all the Kafka tables in the cluster
```
┌─name────────────────┬─type──────────┬
│ database            │ String        │
│ name                │ String        │
│ uuid                │ String        │
│ kafka_cluster       │ String        │
│ topics              │ Array(String) │
│ consumer_group      │ String        │
│ num_consumers       │ UInt32        │
│ consumer_tables     │ Array(String) │
│ consumer_hosts      │ Array(String) │
│ consumer_partitions │ Array(String) │
│ consumer_offsets    │ Array(String) │
└─────────────────────┴───────────────┴
```

**cnch_kafka_tasks**

A separate document about the system table for Cnch Kafka is here
```
┌─name────────────────┬─type──────────┬
│ database            │ String        │
│ name                │ String        │
│ uuid                │ String        │
│ kafka_cluster       │ String        │
│ topics              │ Array(String) │
│ consumer_group      │ String        │
│ is_running          │ UInt8         │
│ assigned_partitions │ Array(String) │
└─────────────────────┴───────────────┴
```

**resource_groups**

- name = resource_group name. This is identical to the virtual_warehouse name in the current Virtual Warehouse Queue implementation
```
─name───────────────────┬─type──────┬─flags─┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ name                   │ String   │       │              │                    │         │                  │
│ enable                 │ UInt8    │       │              │                    │         │                  │
│ can_run_more           │ UInt8    │       │              │                    │         │                  │
│ can_queue_more         │ UInt8    │       │              │                    │         │                  │
│ soft_max_memory_usage  │ Int64    │       │              │                    │         │                  │
│ cached_memory_usage    │ Int64    │       │              │                    │         │                  │
│ max_concurrent_queries │ Int32    │       │              │                    │         │                  │
│ running_queries        │ Int32    │       │              │                    │         │                  │
│ max_queued             │ Int32    │       │              │                    │         │                  │
│ queued_queries         │ Int32    │       │              │                    │         │                  │
│ max_queued_waiting_ms  │ Int32    │       │              │                    │         │                  │
│ priority               │ Int32    │       │              │                    │         │                  │
│ parent_resource_group  │ String   │       │              │                    │         │                  │
│ last_used              │ DateTime │       │              │                    │         │                  │
│ in_use                 │ UInt8    │       │              │                    │         │                  │
│ queued_time_total_ms   │ UInt64   │       │              │                    │         │                  │
│ running_time_total_ms  │ UInt64   │       │              │                    │         │                  │
└────────────────────────┴──────────┴───────┴──────────────┴────────────────────┴─────────┴──────────────────
```

**cnch_parts_info**

```
┌─name───────────────┬─type───┬
│ database           │ String │
│ table              │ String │
│ partition_id       │ String │
│ partition          │ String │
│ first_partition    │ String │
│ total_parts_number │ UInt64 │
│ total_parts_size   │ UInt64 │
│ total_rows_count   │ UInt64 │
└────────────────────┴────────┴
```

- partition_id: the partition_id string 20210404
- partition: the partition name '2021-04-04'
- first_partition: the value of first partition column '2021-04-04' (partition expression can contain multiple columns)
- total_parts_number: the total number of parts in partition
- total_parts_size: the total size in byte of the partition
- total_rows_count: the total rows count of the partition

Notes
1. This table is implemented by the partCache so not all tables will appear in this table.

**cnch_columns**

Clickhouse document
```
┌─name────────────────────┬─type───┬
│ database                │ String │
│ table                   │ String │
│ name                    │ String │
│ table_uuid              │ UUID   │
│ type                    │ String │
│ flags                   │ String │
│ default_kind            │ String │
│ default_expression      │ String │
│ data_compressed_bytes   │ UInt64 │
│ data_uncompressed_bytes │ UInt64 │
│ marks_bytes             │ UInt64 │
│ comment                 │ String │
│ is_in_partition_key     │ UInt8  │
│ is_in_sorting_key       │ UInt8  │
│ is_in_primary_key       │ UInt8  │
│ is_in_sampling_key      │ UInt8  │
│ compression_codec       │ String │
└─────────────────────────┴────────┴
```

- type: type of the column
- flags: B if the column is BloomSet() otherwise empty refer to StorageSystemCnchColumns.cpp:90
- default_kind: if the column has default expression, its value could be default, materialized, alias
- default_expression: the default expression of the column in case the column has a default expression
- data_compressed_bytes: the compressed size of the column in byte
- data_uncompressed_bytes: the uncompressed size of the column in byte
- marks_bytes: the size of marks in byte
- comment: the comment of the column if defined
- is_in_partition_key: if the column is in partition key
- is_in_sorting_key: if the column is in sorting key
- is_in_primary_key: if the column is in primary key
- is_in_sampling_key: if the column is in sampling key
- compression_codec: the compression codec name

**cnch_transactions**

Contains information about transaction records for non read-only transaction.
```
┌─name────────┬─type─────┬
│ txn_id      │ UInt64   │
│ create_time │ DateTime │
│ commit_ts   │ UInt64   │
│ status      │ String   │
│ priority    │ String   │
│ location    │ String   │
│ initiator   │ String   │
└─────────────┴──────────┴
```

- txn_id: transaction id of the transaction
- create_time: the creation time of the transaction in human readable format 
- commit_ts: the commit timestamp of the transaction
- status: the status of the transaction: Running, Finished, Aborted, Inactive, Unknown
- priority: the priority of the transaction: Low or High
- location: location of transaction initiator, format: ip:rpc_port 10.130.48.166:19542
- initiator: the string that identifies the transaction's initiator, for example Kafka, Worker, Merge

**cnch_dictionaries**

Contains information about cnch dictionaries
```
┌─name────────┬─type───┬
│ database    │ String │
│ name        │ String │
│ uuid        │ UUID   │
│ definition  │ String │
│ is_detached │ UInt8  │
└─────────────┴────────┴
```

- definition: the CREATE query of the dictionary
- is_detached: if the dictionary is detached or not

**mutations**

Contains information about the status of running mutations in the MergeTree tables and CnchMergeTree tables
```
┌─name───────────────────────┬─type──────────┬
│ database                   │ String        │
│ table                      │ String        │
│ mutation_id                │ String        │
│ command                    │ String        │
│ create_time                │ DateTime      │
│ cnch                       │ UInt8         │
│ block_numbers.partition_id │ Array(String) │
│ block_numbers.number       │ Array(Int64)  │
│ parts_to_do                │ Int64         │
│ is_done                    │ UInt8         │
│ latest_failed_part         │ String        │
│ latest_fail_time           │ DateTime      │
│ latest_fail_reason         │ String        │
└────────────────────────────┴───────────────┴
```

- mutation_id: transaction id of the mutation
- command: mutation command
- create_time: create time of the mutation
- cnch: 1 if the table is CnchTable
- block_numbers.partition_id: empty for now
- block_numbers.number: empty for now
- parts_to_do: number of parts that haven't been mutated in mutation job
- is_done: if the mutation job is done
- latest_failed_part: empty for now
- latest_fail_time: empty for now
- latest_fail_reason: empty for now

**manipulation**

Contain information about manipulation list. Each of manipulation list represent a Merge or Mutate task
```
┌─name────────────────────────┬─type──────────┬
│ type                        │ String        │
│ task_id                     │ String        │
│ related_node                │ String        │
│ database                    │ String        │
│ table                       │ String        │
│ uuid                        │ UUID          │
│ elapsed                     │ Float64       │
│ progress                    │ Float64       │
│ num_parts                   │ UInt64        │
│ source_part_names           │ Array(String) │
│ result_part_names           │ Array(String) │
│ partition_id                │ String        │
│ total_size_bytes_compressed │ UInt64        │
│ total_size_marks            │ UInt64        │
│ total_rows_count            │ UInt64        │
│ bytes_read_uncompressed     │ UInt64        │
│ bytes_written_uncompressed  │ UInt64        │
│ rows_read                   │ UInt64        │
│ rows_written                │ UInt64        │
│ columns_written             │ UInt64        │
│ memory_usage                │ UInt64        │
│ thread_number               │ UInt64        │
└─────────────────────────────┴───────────────┴
```

- type: the type of manipulation. Mutate/Merge/Clustering...
- task_id: the task id of manipulation task
- related_node: the worker where the task is executed
- elapsed: the elapse time in second since the time the Manipulation is created
- progress: 
- num_parts: the number of source part
- source_part_names: the name of source part that manipulation will work on
- result_part_names: the name of result part that manipulation will create
- partition_id: the partition id for the first source part
- total_size_bytes_compressed: the total bytes of source part on disk
- total_size_marks: the total mark count of the source part
- total_rows_count: the total row count of the source part
- bytes_read_uncompressed: 
- bytes_written_uncompressed: 
- rows_read: 
- rows_written: 
- columns_written: 
- memory_usage: 
- thread_number:   the poco Thread number of the merge mutate thread

**bg_threads**

Contains information about CnchBackGroundThread
```
┌─name─────────┬─type─────┬
│ type         │ String   │
│ database     │ String   │
│ table        │ String   │
│ uuid         │ UUID     │
│ status       │ String   │
│ startup_time │ DateTime │
│ num_wakeup   │ UInt64   │
└──────────────┴──────────┴
```

- type: type of background thread: PartGCThread, MergeSelectThread, MemoryBufferManager ...
- status: status of thread Running...
- startup_time: startup time of thread
- num_wakeup: the number of times the thread wakes up (run() method is called)

**cnch_table_host**

Contains info of server a query should be routed to based on table name
```
┌─name──────┬─type───┬
│ database  │ String │
│ name      │ String │
│ uuid      │ String │
│ host      │ String │
│ tcp_port  │ UInt16 │
│ http_port │ UInt16 │
│ rpc_port  │ UInt16 │
└───────────┴────────┴
```

- host : host address, 127.0.0.1
- tcp_port: tcp port of host
- http_port: http port of host
- rpc_port: rpc port of host

**global_gc_manager**

Contains uuid of the table that are schedule to global_gc (drop meta data, drop datapath... and everything related to a table)
```
┌─name──────────┬─type─┬
│ deleting_uuid │ UUID │
└───────────────┴──────┴
```

- deleting_uuid: the uuid of table that are scheduled to delete in local server
Inorder to have other information like database and name, it can be joined with system.cnch_table_history

**dm_bg_jobs**

Contains the info of jobs that are managed by DaemonManager, used for debugging purpose
```
┌─name────────────┬─type─────┬
│ type            │ String   │
│ database        │ String   │
│ table           │ String   │
│ uuid            │ UUID     │
│ host_port       │ String   │
│ status          │ String   │
│ expected_status │ String   │
│ last_start_time │ DateTime │
└─────────────────┴──────────┴
```

- type: jobs type includes MergeMutateThread, PartGCThread, ConsumerManager, MemoryBuffer
- status: status of the job, could be Running, Stopped, Removed
- expected_status: the expected status of the job. As the job sometimes can transition from one status to another. At the transition time, the status is different than expected status
- last_start_time: the timestamp of the last time the start method on the job is called

**virtual_warehouses**

Contains the information about virtual warehouse
```
┌─name───────────────────┬─type───┬
│ name                   │ String │
│ uuid                   │ String │
│ type                   │ String │
│ active_worker_groups   │ UInt32 │
│ active_workers         │ UInt32 │
│ auto_suspend           │ UInt32 │
│ auto_resume            │ UInt32 │
│ num_workers            │ UInt32 │
│ min_worker_groups      │ UInt32 │
│ max_worker_groups      │ UInt32 │
│ max_concurrent_queries │ UInt32 │
│ max_queued_queries     │ UInt32 │
│ max_queued_waiting_ms  │ UInt32 │
│ vw_schedule_algo       │ String │
└────────────────────────┴────────┴
```

- active_worker_groups: the number of current running worker groups.
- active_workers: the total number of current running workers.
- auto_suspend: not used in cnch yet.
- auto_resume: not used in cnch yet.
- min_worker_groups: the minimum number of worker groups of the vw.
- max_worker_groups: the maximal number of worker groups of the vw.
- max_concurrent_queries: the number of maximal concurrent running queries. 
- max_queued_queries: the number of maximal queued queries in the vw.
- vw_schedule_algo: not used in cnch yet.

**workers**

```
┌─name─────────────────┬─type─────┬
│ worker_id            │ String   │
│ host                 │ String   │
│ tcp_port             │ UInt16   │
│ rpc_port             │ UInt16   │
│ http_port            │ UInt16   │
│ exchange_port        │ UInt16   │
│ exchange_status_port │ UInt16   │
│ vw_name              │ String   │
│ worker_group_id      │ String   │
│ query_num            │ UInt32   │
│ cpu_usage            │ Float64  │
│ memory_usage         │ Float64  │
│ disk_space           │ UInt64   │
│ memory_available     │ UInt64   │
│ last_update_time     │ DateTime │
└──────────────────────┴──────────┴
```

Info about worker fetch from Resource Manager
- worker_id: id of worker
- cpu_usage: the current cpu usage of the worker
- memory_usage: the current memory usage of the worker
- last_update_time: the latest heartbeat timestamp of the worker

