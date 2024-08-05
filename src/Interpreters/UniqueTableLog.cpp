#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/UniqueTableLog.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

NamesAndTypesList UniqueTableLogElement::getNamesAndTypes()
{
    auto log_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"EMPTY",               static_cast<Int8>(EMPTY)},
        {"ERROR",               static_cast<Int8>(ERROR)}});

    return
    {
        {"database",            std::make_shared<DataTypeString>()},
        {"table",               std::make_shared<DataTypeString>()},

        {"event_type",          std::move(log_type)},
        {"event_date",          std::make_shared<DataTypeDate>()},
        {"event_time",          std::make_shared<DataTypeDateTime>()},
        {"txn_id",              std::make_shared<DataTypeUInt64>()},
        {"task_info",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"event_info",          std::make_shared<DataTypeString>()},

        {"duration_ms",         std::make_shared<DataTypeUInt64>()},
        {"metric",              std::make_shared<DataTypeUInt64>()},

        {"has_error",           std::make_shared<DataTypeUInt8>()},
        {"event_msg",           std::make_shared<DataTypeString>()}
    };
}

void UniqueTableLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(database);
    columns[i++]->insert(table);

    columns[i++]->insert(type);
    columns[i++]->insert(DateLUT::sessionInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(txn_id);

    Array task_info_details;
    for (const auto & info_detail : task_info.getTaskInfoDetail())
        task_info_details.emplace_back(info_detail);
    columns[i++]->insert(task_info_details);
    columns[i++]->insert(event_info);

    columns[i++]->insert(UInt64(duration_ms));
    columns[i++]->insert(UInt64(metric));

    columns[i++]->insert(UInt64(has_error));
    columns[i++]->insert(event_msg);
}

static String toString(TaskInfo::TaskType type)
{
   switch (type)
   {
        case TaskInfo::DEDUP_TASK:        return "DEDUP_TASK";
        case TaskInfo::MERGE_TASK:        return "MERGE_TASK";
        case TaskInfo::DATA_CHECKER_TASK: return "DATA_CHECKER_TASK";
        default:                          return "UNKNOWN_TASK";
   }
}

Names TaskInfo::getTaskInfoDetail() const
{
    Names ret;
    ret.emplace_back(toString(task_type));
    ret.emplace_back(dedup_gran.partition_id);
    ret.emplace_back(toString(dedup_gran.bucket_number));
    return ret;
}

namespace UniqueTable
{
UniqueTableLogElement createUniqueTableLog(UniqueTableLogElement::Type type, const StorageID & storage_id, bool has_error)
{
    UniqueTableLogElement elem;
    elem.database = storage_id.getDatabaseName();
    elem.table = storage_id.getTableName();
    elem.type = type;
    elem.event_time = time(nullptr);
    elem.has_error = has_error;
    return elem;
}

String formatUniqueKey(const String & unique_index_str_, const StorageMetadataPtr & metadata_snapshot)
{
    WriteBufferFromOwnString msg;
    ReadBufferFromString unique_index_str(unique_index_str_);

    msg << "[";
    size_t index = 0;
    for (const auto & col : metadata_snapshot->getUniqueKey().sample_block)
    {
        const auto & type = col.type;
        SerializationPtr serialization = type->getDefaultSerialization();
        auto tmp = col.type->createColumn();
        serialization->deserializeMemComparable(*tmp, unique_index_str);
        if (index++ != 0)
            msg << ", ";

        msg << col.name << ": " << (*tmp)[0].dump();
    }
    msg << "]";

    return std::move(msg.str());
}
}

}
