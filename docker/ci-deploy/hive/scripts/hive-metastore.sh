/opt/hive/bin/hive --service metastore &

sleep 10

# check safe mode status
function checkSafeModeStatus() {
    hadoop_status=$(hdfs dfsadmin -safemode get)
    if [[ $hadoop_status == *"Safe mode is OFF"* ]]; then
        echo "Safe mode is now OFF"
        return 0
    else
        echo "Safe mode is still ON. Waiting..."
        return 1
    fi
}

safe_mode_on=true
while $safe_mode_on; do
    if checkSafeModeStatus; then
        safe_mode_on=false
    fi
    sleep 5
done

# simple csv external hive table
hadoop fs -mkdir -p /user/byconity/csv/simple_csv_tbl
hadoop fs -put /mnt/scripts/all.csv /user/byconity/csv/simple_csv_tbl

echo "create hive tables"
hive -f /mnt/scripts/create_simple_table.sql

# Avoid container exit
while true; do
    sleep 5
done