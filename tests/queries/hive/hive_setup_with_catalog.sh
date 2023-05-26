#!/bin/bash
set -x -e
export HIVE_METASTORE='thrift://hive-metastore:9083'
clickhouse-client --time  --tenant_id "${TENANT_ID}"  --query "create external catalog hive_hdfs properties hive.metastore.uri='${HIVE_METASTORE}'   , type = 'hive'"

clickhouse-client --time --query "show external catalogs"