#include <Storages/Hive/HivePartition.h>

namespace DB
{

HivePartition::HivePartition(const String & partition_id_, HivePartitionInfo & info)
             : partition_id(partition_id_)
             , info_(info)
{

}

HivePartition::~HivePartition()
{
}

const String HivePartition::getID()
{
    return partition_id;
}

const String HivePartition::getTablePath()
{
    return info_.table_path;
}

const String HivePartition::getPartitionPath()
{
    return info_.getLocation();
}

const String HivePartition::getTableName()
{
    return info_.tableName;
}

const String HivePartition::getDBName()
{
    return info_.dbName;
}

int32_t HivePartition::getCreateTime()
{
    return info_.createTime;
}

int32_t HivePartition::getLastAccessTime()
{
    return info_.lastAccessTime;
}

const std::vector<String> HivePartition::getValues()
{
    return info_.values;
}

const String HivePartition::getInputFormat()
{
    return info_.inputFormat;
}

const String HivePartition::getOutputFromat()
{
    return info_.outputFormat;
}

const std::vector<String> HivePartition::getPartsName()
{
    return info_.parts_name;
}




}
