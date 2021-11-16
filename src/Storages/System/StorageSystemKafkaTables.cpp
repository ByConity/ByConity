#include <Common/config.h>
#if USE_RDKAFKA

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemKafkaTables.h>
#include <Storages/Kafka/StorageHaKafka.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/SourceFromInputStream.h>

namespace DB
{

StorageSystemKafkaTables::StorageSystemKafkaTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    auto consumer_switch_datatype = std::make_shared<DataTypeEnum8>(
            DataTypeEnum8::Values {
            {"OFF",         static_cast<Int8>(StorageHaKafka::Status::OFF)},
            {"ON",          static_cast<Int8>(StorageHaKafka::Status::ON)},
            });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "database",               std::make_shared<DataTypeString>()  },
        { "table",                  std::make_shared<DataTypeString>()  },
        { "is_leader",              std::make_shared<DataTypeUInt8>()   },
        { "is_readonly",            std::make_shared<DataTypeUInt8>()   },
        { "is_session_expired",     std::make_shared<DataTypeUInt8>()   },
        { "zookeeper_path",         std::make_shared<DataTypeString>()  },
        { "replica_name",           std::make_shared<DataTypeString>()  },
        { "replica_path",           std::make_shared<DataTypeString>()  },
        /// TODO:
        /// { "total_replicas",         std::make_shared<DataTypeUInt8>()   },
        /// { "active_replicas",        std::make_shared<DataTypeUInt8>()   },
        { "kafka_cluster",          std::make_shared<DataTypeString>()  },
        { "topics",                 std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "group_name",             std::make_shared<DataTypeString>()  },
        { "assigned_partitions",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "consumer_switch",        consumer_switch_datatype            },
        { "dependencies",           std::make_shared<DataTypeUInt32>()  },
        { "num_consumers",          std::make_shared<DataTypeUInt32>()  },
        { "is_consuming",           std::make_shared<DataTypeUInt8>()   },
        { "last_exception",         std::make_shared<DataTypeString>()  },
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemKafkaTables::read(
    const Names & /* column_names */,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    /// check(column_names);

    std::map<String, std::map<String, StoragePtr>> kafka_tables;
    for (auto & db : DatabaseCatalog::instance().getDatabases())
    {
        context->checkAccess(AccessType::SHOW_DATABASES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            if (dynamic_cast<const StorageHaKafka *>(iterator->table().get()))
                kafka_tables[db.first][iterator->name()] = iterator->table();
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : kafka_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database(std::move(col_database_mut));
    ColumnPtr col_table(std::move(col_table_mut));

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return Pipe();

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
    }

    MutableColumns res_columns = getInMemoryMetadataPtr()->getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0, size = col_database->size(); i < size; ++i)
    {
        StorageHaKafka::Status status;

        auto & explicit_table = kafka_tables[(*col_database)[i].safeGet<const String &>()][(*col_table)[i].safeGet<const String &>()];
        auto ha_kafka = dynamic_cast<StorageHaKafka*>(explicit_table.get());
        ha_kafka->getStatus(status);

        size_t col_num = 2;
        res_columns[col_num++]->insert(status.is_leader);
        res_columns[col_num++]->insert(status.is_readonly);
        res_columns[col_num++]->insert(status.is_session_expired);
        res_columns[col_num++]->insert(status.zookeeper_path);
        res_columns[col_num++]->insert(status.replica_name);
        res_columns[col_num++]->insert(status.replica_path);
        res_columns[col_num++]->insert(status.kafka_cluster);

        Array topics;
        topics.reserve(status.topics.size());
        for (auto & t : status.topics)
            topics.push_back(t);
        res_columns[col_num++]->insert(topics);

        res_columns[col_num++]->insert(status.group_name);

        Array assigned_partitions;
        assigned_partitions.reserve(status.assigned_partitions.size());
        for (auto & p : status.assigned_partitions)
            assigned_partitions.push_back(p);
        res_columns[col_num++]->insert(assigned_partitions);

        res_columns[col_num++]->insert(UInt8(status.consumer_switch));
        res_columns[col_num++]->insert(status.dependencies);
        res_columns[col_num++]->insert(status.num_consumers);
        res_columns[col_num++]->insert(status.is_consuming);
        res_columns[col_num++]->insert(status.last_exception);
    }

    Block res = getInMemoryMetadataPtr()->getSampleBlock().cloneWithoutColumns();
    size_t col_num = 0;
    res.getByPosition(col_num++).column = col_database;
    res.getByPosition(col_num++).column = col_table;
    size_t num_columns = res.columns();
    while (col_num < num_columns)
    {
        res.getByPosition(col_num).column = std::move(res_columns[col_num]);
        ++col_num;
    }

    return Pipe(std::make_shared<SourceFromInputStream>(std::make_shared<OneBlockInputStream>(res)));
}

} // end of namespace DB

#endif
