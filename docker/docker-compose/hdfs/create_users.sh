#!/bin/bash
set -x
docker exec hdfs-namenode hdfs dfs -mkdir /user
docker exec hdfs-namenode hdfs dfs -mkdir /user/clickhouse
docker exec hdfs-namenode hdfs dfs -chown clickhouse /user/clickhouse
docker exec hdfs-namenode hdfs dfs -chmod -R 775 /user/clickhouse 

