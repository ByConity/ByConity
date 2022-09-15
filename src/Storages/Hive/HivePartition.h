#pragma once

#include <Core/Types.h>
#include <hivemetastore/hive_metastore_types.h>

namespace DB
{

using namespace Apache::Hadoop::Hive;

struct HivePartitionInfo
{
    String dbName;
    String tableName;
    String partition_path;
    String table_path;
    int32_t createTime;
    int32_t lastAccessTime;
    std::vector<String> values;
    String inputFormat;
    String outputFormat;
    std::vector<FieldSchema> cols;
    std::vector<String> parts_name;

    HivePartitionInfo(const String & dbName_, const String & tableName_, const String & partition_path_, const String & table_path_, int32_t & createTime_, int32_t & lastAccessTime_,
                      std::vector<String> & values_, const String & inputFormat_, const String & outputFormat_, std::vector<FieldSchema> & cols_, std::vector<String> & parts_name_)
                    : dbName(dbName_)
                    , tableName(tableName_)
                    , partition_path(partition_path_)
                    , table_path(table_path_)
                    , createTime(createTime_)
                    , lastAccessTime(lastAccessTime_)
                    , values(values_)
                    , inputFormat(inputFormat_)
                    , outputFormat(outputFormat_)
                    , cols(cols_)
                    , parts_name(parts_name_)
    {}
    HivePartitionInfo() = default;

    const std::vector<String> getPartsName()
    {
        return parts_name;
    }
    const String getLocation()
    {
        return partition_path;
    }


};

class HivePartition
{
    public:
        HivePartition(const String & partition_id , HivePartitionInfo & info_);
        ~HivePartition();

    public:
        const String getID();
        const String getTablePath();
        const String getPartitionPath();
        const String getTableName();
        const String getDBName();
        int32_t getCreateTime();
        int32_t getLastAccessTime();
        const std::vector<String> getValues();
        const String getInputFormat();
        const String getOutputFromat();
        const std::vector<String> getPartsName();



    private:
        String partition_id;
        HivePartitionInfo info_;
};

}
