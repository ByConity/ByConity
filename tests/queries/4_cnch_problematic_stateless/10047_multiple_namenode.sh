#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

nnproxy="data.bytehouse.nnproxy-hostnetwork.service.lf"
if [ -n  "$HDFS_NNPROXY" ]; then
    nnproxy=$HDFS_NNPROXY
fi

if [ ! -n "$hdfs_command" ]; then  hdfs_command="./cnch/bin/hdfs_command"; fi

dir=`uuid`
root_path="/user/cnch/multi_namenode_ci/"$dir

# create root path
$hdfs_command --nnproxy $nnproxy --mkdir $root_path 1>/dev/null

# add root path into system.root_paths
${CLICKHOUSE_CLIENT} --query "INSERT INTO system.root_paths(root_path) values('$root_path')"

# create table that use the root path
database_uuid=`uuid`
database_uuid=${database_uuid//-/_}
database="multiple_namenode_database_"$database_uuid
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE $database"

table_name="test_multiple_namenode_"$dir
table_name=${table_name//-/_}
engine="ENGINE = CnchMergeTree() PARTITION BY toDate(EventDate) ORDER BY c1"
root_paths="ROOT PATHS '$root_path'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $database.$table_name(c1 UInt32, EventDate Date) $engine $root_paths"

# add default root path
query="SELECT root_path FROM system.root_paths WHERE id=0"
default_root_path=`${CLICKHOUSE_CLIENT} --query "$query"`
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $database.$table_name ADD ROOT PATHS '$default_root_path'"

#insert data
date='2021-01-04'
for i in {1..10}
do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO $database.$table_name VALUES($i, '$date')"
done

# query all data
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $database.$table_name ORDER BY c1"

# detach partition
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $database.$table_name DETACH PARTITION '$date'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $database.$table_name ORDER BY c1"

# attach partition
# ${CLICKHOUSE_CLIENT} --query "ALTER TABLE $database.$table_name ATTACH PARTITION '$date'"
# ${CLICKHOUSE_CLIENT} --query "SELECT * FROM $database.$table_name ORDER BY c1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $database.$table_name"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE $database"
dir_list=`$hdfs_command --nnproxy $nnproxy --ls $root_path`
echo $dir_list | while read table_uuid;
do
    $hdfs_command --nnproxy $nnproxy --rmr $table_uuid 1>/dev/null
done
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.root_paths WHERE root_path='$root_path'"

# remove root path
$hdfs_command --nnproxy $nnproxy --rmdir $root_path 1>/dev/null
