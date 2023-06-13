#!/bin/bash
set -e -x
docker exec ${COMPOSE_PROJECT_NAME}-hdfs-namenode hdfs dfs -mkdir -p /user/clickhouse
docker exec ${COMPOSE_PROJECT_NAME}-hdfs-namenode hdfs dfs -chown clickhouse /user/clickhouse
docker exec ${COMPOSE_PROJECT_NAME}-hdfs-namenode hdfs dfs -chmod -R 775 /user/clickhouse 
