# Hive quick start

## Start hive
```sh
docker-compose --env-file hadoop-hive.env -p hive up
```

## How to access hive
```sh
docker-compose --env-file hadoop-hive.env exec hive-metastore bash
$ hive
```

alternative
```sh
docker-compose --env-file hadoop-hive.env exec hive-server bash
$ /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

## Byconity client
```sh
echo "127.0.0.1 namenode" >> /etc/hosts
```

```
CREATE TABLE hive.docker
(
    `name` String,
    `value` Int32,
    `event_date` String
)
ENGINE = CnchHive('thrift://localhost:9183', 'par_db', 'par_tbl')
PARTITION BY event_date
```
