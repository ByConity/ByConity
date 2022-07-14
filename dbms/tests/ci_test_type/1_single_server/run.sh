set -e -x -a

APP_ROOT=/home/code
BIN_PATH=/clickhouse/bin
ARTIFACT_FOLDER_PATH=/test_output

# set unique value in xml
UNIQUE_VALUE=$(cat /proc/sys/kernel/random/uuid)
UNIQUE_VALUE_TSO=$(cat /proc/sys/kernel/random/uuid)
BYTEJOURNAL_CNCH_PREFIX=cnch_ci_${UNIQUE_VALUE}_
SERVER_ELECTION_POINT=server_point_${UNIQUE_VALUE}
CATALOG_NAMESPACE=${UNIQUE_VALUE}

sed -i "s/cnch_ci_random_suffix_replace_me_/cnch_ci_random_suffix_${UNIQUE_VALUE}_/"  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/*.xml
sed -i "s/random_suffix_replace_me/${UNIQUE_VALUE_TSO}/"  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/tso*.xml
sed -i "s/random_suffix_replace_me/${UNIQUE_VALUE}/"  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/*.xml
HDFS_PATH=${HDFS_PATH:-/user/cnch_ci/${UNIQUE_VALUE}/}
sed -i "s#server_hdfs_disk_replace_me#${HDFS_PATH}#"  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/*.xml

# create log folder
SERVICES=("tso0" "tso1" "tso2" "server" "vw-default" "vw-write" "daemon-manager" "resource-manager0" "resource-manager1" "udf_manager" "udf_script")
for service in "${SERVICES[@]}"; do
      mkdir -p "${ARTIFACT_FOLDER_PATH}/${service}"
done

ASAN_OPTIONS=halt_on_error=false,log_path=${ARTIFACT_FOLDER_PATH}/tso0/asan.tso0.log nohup .${BIN_PATH}/tso-server --config-file  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/tso0.xml >/dev/null 2>&1 &
sleep 5
ASAN_OPTIONS=halt_on_error=false,log_path=${ARTIFACT_FOLDER_PATH}/daemon-manager/asan.daemon-manager.log nohup .${BIN_PATH}/daemon-manager --config-file  ${APP_ROOT}/dbms/tests/ci_test_type/1_single_server/etc/daemon-manager.xml >/dev/null 2>&1 &
sleep 5