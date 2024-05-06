#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/System/StorageSystemKafkaTasks.h>

#include <Access/ContextAccess.h>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <CloudServices/CnchBGThreadsMap.h>
#include <Storages/Kafka/CnchKafkaConsumeManager.h>
#include <Storages/Kafka/StorageCloudKafka.h>
#include <Storages/Kafka/StorageCnchKafka.h>

namespace DB
{
NamesAndTypesList StorageSystemKafkaTasks::getNamesAndTypes()
{
    return {
        { "database",                   std::make_shared<DataTypeString>()  },
        { "name",                       std::make_shared<DataTypeString>()  },
        /// uuid is not supported for local tables on worker side now
        /// { "uuid",                       std::make_shared<DataTypeString>()  },
        { "kafka_cluster",              std::make_shared<DataTypeString>()  },
        { "topics",                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "consumer_group",             std::make_shared<DataTypeString>()  },
        { "is_running",                 std::make_shared<DataTypeUInt8>()   },
        { "assigned_partitions",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
    };
}

static void fillDataOnWorker(MutableColumns & res_columns, ContextPtr context)
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// get all kafka tables
    std::unordered_set<StoragePtr> kafka_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(context))
    {
        if (db.first == DatabaseCatalog::TEMPORARY_DATABASE || db.first == "system")
            continue;

        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first))
            continue;

        {
            for (auto iter = db.second->getTablesIterator(context); iter->isValid(); iter->next())
            {
                if (dynamic_cast<const StorageCloudKafka *>(iter->table().get()))
                    kafka_tables.emplace(iter->table());
            }
        }
    }

    /// each kafka table presents an unique consumer task
    for (const auto & table : kafka_tables)
    {
        const StorageCloudKafka * kafka_table = dynamic_cast<const StorageCloudKafka *>(table.get());
        if (!kafka_table)
            continue;

        cppkafka::TopicPartitionList tpl = kafka_table->getConsumerAssignment();

        KafkaTableInfo table_info;
        kafka_table->getKafkaTableInfo(table_info);

        size_t col_num = 0;
        res_columns[col_num++]->insert(table_info.database);
        res_columns[col_num++]->insert(table_info.table);
        /// res_columns[col_num++]->insert(table_info.uuid);
        res_columns[col_num++]->insert(table_info.cluster);

        Array topics;
        for (auto & topic : table_info.topics)
            topics.emplace_back(topic);
        res_columns[col_num++]->insert(topics);

        res_columns[col_num++]->insert(table_info.consumer_group);
        res_columns[col_num++]->insert((!tpl.empty() && kafka_table->getStreamStatus()) ? 1 : 0);

        if (tpl.empty())
            res_columns[col_num++]->insertDefault();
        else
        {
            Array assignment;
            for (auto & tp : tpl)
                assignment.emplace_back(tp.get_topic() + "[" + std::to_string(tp.get_partition())
                                        + ":" + std::to_string(tp.get_offset()) + "]");
            res_columns[col_num++]->insert(assignment);
        }
    }
}

static void fillDataOnServer(MutableColumns & res_columns, ContextPtr context)
{
    auto catalog = context->getCnchCatalog();
    if (!catalog)
        throw Exception("No catalog client found", ErrorCodes::LOGICAL_ERROR);

    auto threads = context->getCnchBGThreadsMap(CnchBGThread::Consumer)->getAll();
    for (const auto & iter : threads) {
        auto * manager = dynamic_cast<CnchKafkaConsumeManager*>(iter.second.get());
        if (!manager)
            continue;
        const auto & consumer_infos = manager->getConsumerInfos();
        if (consumer_infos.empty())
            return;

        auto storage = catalog->getTableByUUID(*context, UUIDHelpers::UUIDToString(iter.first), TxnTimestamp::maxTS());
        auto * kafka_table = dynamic_cast<StorageCnchKafka*>(storage.get());
        if (!kafka_table)
            return;

        KafkaTableInfo table_info;
        kafka_table->getKafkaTableInfo(table_info);

        for (const auto & consumer : consumer_infos)
        {
            size_t col_num = 0;

            res_columns[col_num++]->insert(table_info.database);
            res_columns[col_num++]->insert(table_info.table + consumer.table_suffix);
            /// res_columns[col_num++]->insert(table_info.uuid);
            res_columns[col_num++]->insert(table_info.cluster);

            Array topics;
            for (auto & topic : table_info.topics)
                topics.emplace_back(topic);
            res_columns[col_num++]->insert(topics);

            res_columns[col_num++]->insert(table_info.consumer_group);
            res_columns[col_num++]->insert(consumer.is_running ? 1 : 0);

            Array assigned_partitions;
            assigned_partitions.reserve(consumer.partitions.size());
            for (const auto & tp : consumer.partitions)
                assigned_partitions.emplace_back(tp.get_topic() + "[" + std::to_string(tp.get_partition())
                                                 + ":" + std::to_string(tp.get_offset()) + "]");
            res_columns[col_num++]->insert(assigned_partitions);
        }
    }
}

void StorageSystemKafkaTasks::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & ) const
{
    if (context->getServerType() == ServerType::cnch_worker)
        fillDataOnWorker(res_columns, context);
    else if (context->getServerType() == ServerType::cnch_server)
        fillDataOnServer(res_columns, context);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "system.kafka_tasks only supported on cnch-server or cnch-worker");
}
}
#endif
