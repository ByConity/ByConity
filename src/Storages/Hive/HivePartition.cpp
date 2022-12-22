#include <Storages/Hive/HivePartition.h>

namespace DB
{
HivePartition::HivePartition(const String & partition_id_, HivePartitionInfo & info_) : partition_id(partition_id_), info(info_)
{
}

HivePartition::~HivePartition() = default;

const String & HivePartition::getID()
{
    return partition_id;
}

const String & HivePartition::getTablePath() const
{
    return info.table_path;
}

const String & HivePartition::getPartitionPath()
{
    return info.getLocation();
}

const String & HivePartition::getTableName() const
{
    return info.table_name;
}

const String & HivePartition::getDBName() const
{
    return info.db_name;
}

int32_t HivePartition::getCreateTime() const
{
    return info.create_time;
}

int32_t HivePartition::getLastAccessTime() const
{
    return info.last_access_time;
}

const std::vector<String> & HivePartition::getValues() const
{
    return info.values;
}

const String & HivePartition::getInputFormat() const
{
    return info.input_format;
}

const String & HivePartition::getOutputFromat() const
{
    return info.output_format;
}

const std::vector<String> & HivePartition::getPartsName() const
{
    return info.parts_name;
}


}
