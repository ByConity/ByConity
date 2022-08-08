set -x
# copy core file to shared folder
CORE_FILE_PATH='/'
ARTIFACT_FOLDER_PATH='/test_output/coredump'
MINIDUMP_FILE_PATH='/var/log/'

for FILES in  $(ls -l ${CORE_FILE_PATH}  | grep -v ^d | grep "core." | awk '{print $9}' )
do
    mkdir -p ${ARTIFACT_FOLDER_PATH}
    for i in FILES:
    do
      gdb -q /clickhouse/bin/clickhouse /${FILES} -ex "set pagination off" -ex bt -ex "quit" |& tee ${ARTIFACT_FOLDER_PATH}/${FILES}_coredump_backtrace.log  # use gdb to get stack information
      python3 /home/code/docker/common_component/lark_bot/extract_gdb_log.py --log-path ${ARTIFACT_FOLDER_PATH}/${FILES}_coredump_backtrace.log
      cp -r /${FILES} ${ARTIFACT_FOLDER_PATH}/.
    done
done

for FILES in  $(find ${MINIDUMP_FILE_PATH} -name "*.dmp" )
do
  for i in FILES:
  do
    echo $FILES
    minidump-2-core $FILES > $FILES.core
    gdb -q /clickhouse/bin/clickhouse $FILES.core -ex "set pagination off" -ex "bt" -ex "quit" > ${FILES}_minidump_backtrace.log
    python3 /home/code/docker/common_component/lark_bot/extract_gdb_log.py --log-path ${FILES}_minidump_backtrace.log
  done
done

# coredump lark bot
python3 /home/code/.codebase/ci_scripts/common_component/lark_bot/main.py
