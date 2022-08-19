set -e -x -a

APP_ROOT=/home/code
BIN_PATH=/clickhouse/bin
ARTIFACT_FOLDER_PATH=/artifacts

# set unique value in xml
UNIQUE_VALUE=$(cat /proc/sys/kernel/random/uuid)
UNIQUE_VALUE_TSO=$(cat /proc/sys/kernel/random/uuid)
BYTEJOURNAL_CNCH_PREFIX=cnch_ci_${UNIQUE_VALUE}_
SERVER_ELECTION_POINT=server_point_${UNIQUE_VALUE}
CATALOG_NAMESPACE=${UNIQUE_VALUE}

sed -i "s/clickhouse_ci_random_suffix_replace_me_/clickhouse_ci_random_suffix_${UNIQUE_VALUE}_/"  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/*.xml
sed -i "s/random_suffix_replace_me/${UNIQUE_VALUE_TSO}/"  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/tso*.xml
sed -i "s/random_suffix_replace_me/${UNIQUE_VALUE}/"  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/*.xml
HDFS_PATH=${HDFS_PATH:-/user/clickhouse_ci/${UNIQUE_VALUE}/}
sed -i "s#server_hdfs_disk_replace_me#${HDFS_PATH}#"  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config//*.xml

IP_ADDRESS=$(hostname -I | cut -d " " -f 1) # container's ipv4 address
sed -i "s#ip_address_replace_me#${IP_ADDRESS}#"  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/*.xml

# create log folder
SERVICES=("tso0" "server" "vw-default" "vw-write" "daemon-manager" "resource-manager0" "resource-manager1" "udf_manager" "udf_script" "clickhouse-test-log")
for service in "${SERVICES[@]}"; do
      mkdir -p "${ARTIFACT_FOLDER_PATH}/${service}"
done
echo 'test'
# create folder to store data as local disk
for ((i=0; i<10; i++))
do
  mkdir -p /cnch/cnch_test/server/data/$i
  mkdir -p /cnch/cnch_test/worker0/data/$i
  mkdir -p /cnch/cnch_test/worker1/data/$i
done

# disable  CLICKHOUSE WATCHDOG
CLICKHOUSE_WATCHDOG_ENABLE=0

# start services
ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/tso0/asan.tso0.log nohup /clickhouse/bin/tso-server --config-file ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/tso0.xml >/dev/null 2>&1 &
#sleep 2
#ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/tso1/asan.tso1.log nohup /clickhouse/bin/tso-server --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/tso1.xml >/dev/null 2>&1 &
#sleep 2
#ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/tso2/asan.tso2.log nohup /clickhouse/bin/tso-server --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/tso2.xml >/dev/null 2>&1 &
sleep 2
#ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/resource-manager0/asan.rm0.log nohup /clickhouse/bin/resource_manager --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/rm0.xml  >/dev/null 2>&1 &
#sleep 2
#ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/resource-manager1/asan.rm1.log nohup /clickhouse/bin/resource_manager --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/rm1.xml  >/dev/null 2>&1 &
#sleep 2
ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/server/asan.server.log nohup /clickhouse/bin/clickhouse-server --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/server.xml >/dev/null 2>&1 &
sleep 2
ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/vw-default/asan.worker-default.log WORKER_ID='default-worker-0' WORKER_GROUP_ID='default' VIRTUAL_WAREHOUSE_ID='vw_default' nohup  /clickhouse/bin/clickhouse-server --config-file   ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/worker0.xml >/dev/null 2>&1 &
sleep 2
ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/vw-write/asan.worker-write.log WORKER_ID='default-worker-1' WORKER_GROUP_ID='write' VIRTUAL_WAREHOUSE_ID='vw_write' nohup  /clickhouse/bin/clickhouse-server --config-file   ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/worker1.xml >/dev/null 2>&1 &
sleep 2
ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/daemon-manager/asan.daemon_manager.log nohup /clickhouse/bin/daemon_manager  --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/daemon-manager.xml  >/dev/null 2>&1 &
sleep 2
# ASAN_OPTIONS=halt_on_error=false,log_path=/artifacts/udf_manager/asan.udf_manager.log nohup /clickhouse/bin/udf_manager_server --config-file  ${APP_ROOT}/.codebase/ci_scripts/cnch_config/1_single_server/config/udf-manager.xml >/dev/null 2>&1 &
# udf_script log has been defiend in server.xml     <udf_path>/builds/dp/artifacts/udf_script</udf_path>
sleep 5

# show service status
ps -aux


