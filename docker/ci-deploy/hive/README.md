# Hive quick start

## How to access hive
```sh
docker-compose exec hive-metastore bash
$ hive
```
**OR**

```sh
docker-compose exec hive-server bash
$ /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

## CREATE TABLE syntax 
You can enter the following queries after connecting to ByConity client using the following command:
`$GITHUB_WORKSPACE/build/programs/clickhouse-client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT_TCP`

### parquet table
```sql
CREATE TABLE hive.par_tbl
(
    `name` String,
    `value` Int32,
    `event_date` String
)
ENGINE = CnchHive('thrift://hive-metastore:9083', 'par_db', 'par_tbl')
PARTITION BY event_date
```

### orc table
```sql
CREATE TABLE hive.orc_tbl
(
    `name` String,
    `value` Int32,
    `event_date` String
)
ENGINE = CnchHive('thrift://hive-metastore:9083', 'orc_db', 'orc_tbl')
PARTITION BY event_date
```