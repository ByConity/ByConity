#include <Common/config.h>
#if USE_RDKAFKA

#include <Parsers/ASTSetQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/Kafka/StorageCloudKafka.h>

#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static StoragePtr createStorageCnchKafka(const StorageFactory::Arguments & args)
{
    if (!args.storage_def->settings)
    {
        throw Exception("CnchKafka requires settings", ErrorCodes::BAD_ARGUMENTS);
    }
    for (auto & change : args.storage_def->settings->as<ASTSetQuery &>().changes)
        addKafkaPrefix(change.name);
    sortKafkaSettings(*args.storage_def->settings);

    KafkaSettings kafka_settings;
    kafka_settings.loadFromQuery(*args.storage_def);

#define CHECK_CNCH_KAFKA_STORAGE_ARGUMENT(PARA_NAME)                \
    if (!kafka_settings.PARA_NAME.changed)                      \
    {                                                           \
        throw Exception(                                        \
                "Required parameter '" #PARA_NAME "' "          \
                "for storage CnchKafka not specified",          \
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);  \
    }
    CHECK_CNCH_KAFKA_STORAGE_ARGUMENT(topic_list)
    CHECK_CNCH_KAFKA_STORAGE_ARGUMENT(group_name)
    CHECK_CNCH_KAFKA_STORAGE_ARGUMENT(format)
#undef CHECK_CNCH_KAFKA_STORAGE_ARGUMENT

    bool is_consumer = endsWith(args.engine_name, "CloudKafka");

    if (is_consumer)
    {
        auto & client_info = args.getLocalContext()->getClientInfo();
        String server_client_host = client_info.current_address.host().toString();
        UInt16 server_client_rpc_port = client_info.rpc_port;

        return StorageCloudKafka::create(
                args.table_id,
                args.getContext(),
                args.columns,
                args.constraints,
                kafka_settings,
                server_client_host,
                server_client_rpc_port);
    }
    else
    {
        return StorageCnchKafka::create(
                args.table_id,
                args.getContext(),
                args.columns,
                args.constraints,
                kafka_settings
                );
    }
}

void registerStorageCnchKafka(StorageFactory & factory)
{
    factory.registerStorage("CnchKafka", createStorageCnchKafka, StorageFactory::StorageFeatures{ .supports_settings = true, });
    factory.registerStorage("CloudKafka", createStorageCnchKafka, StorageFactory::StorageFeatures{ .supports_settings = true, });
}
}

#endif

