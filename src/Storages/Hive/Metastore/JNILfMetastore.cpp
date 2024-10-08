#include "JNILfMetastore.h"
#include <cstdint>
#include <Poco/URI.h>
#include "Parsers/formatTenantDatabaseName.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS
#    include <hudi.pb.h>
#    include <hive_metastore_types.h>
#    include <jni/JNIByteCreatable.h>
#    include <jni/JNIMetaClient.h>
#    include <thrift/protocol/TCompactProtocol.h>
#    include <thrift/transport/TBufferTransports.h>
namespace DB
{
using apache::thrift::protocol::TCompactProtocolT;
using apache::thrift::transport::TMemoryBuffer;


static std::string LF_CLIENT_FACTORY = "org/byconity/las/LasClassFactory";
static std::string LF_CLASS_NAME = "org/byconity/las/LasFormationClient";


std::unordered_map<String, String> JNILfMetastore::buildPropertiesFromHiveSettigns(const CnchHiveSettings & settings)
{
    std::unordered_map<String, String> properties;
    properties.emplace("hive.metastore.uris", settings.lf_metastore_url.value);
    properties.emplace("region", settings.lf_metastore_region.value);
    properties.emplace("access_key", settings.lf_metastore_ak_id.value);
    properties.emplace("secret_key", settings.lf_metastore_ak_secret.value);
    properties.emplace("lf.metastore.catalog", settings.lf_metastore_catalog);

    return properties;
}

JNILfMetastore::JNILfMetastore(const std::unordered_map<String, String> & properties_) : properties(properties_)
{
    Protos::HudiMetaClientParams params;
    for (const auto & p : properties_)
    {
        auto * property = params.mutable_properties()->add_properties();
        property->set_key(p.first);
        property->set_value(p.second);
    }

    jni_client = std::make_unique<JNIMetaClient>(LF_CLIENT_FACTORY, LF_CLASS_NAME, params.SerializeAsString());
}

Strings JNILfMetastore::getAllDatabases()
{
    return jni_client->listDatabases();
}

Strings JNILfMetastore::getAllTables(const std::string & db_name)
{
    return jni_client->listTables(getOriginalDatabaseName(db_name));
}

static String convertTOSToS3(const std::string & path)
{
    Poco::URI uri(path);
    uri.setScheme("s3");
    return uri.toString();
}
std::shared_ptr<ApacheHive::Table> JNILfMetastore::getTable(const String & db_name, const String & table_name)
{
    auto table_raw = jni_client->getTable(getOriginalDatabaseName(db_name), table_name);
    auto transport = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t *>(table_raw.data()), table_raw.size());
    TCompactProtocolT<TMemoryBuffer> protocal(transport);
    auto table = std::make_shared<ApacheHive::Table>();
    table->read(&protocal);
    table->sd.location = convertTOSToS3(table->sd.location);
    return table;
}

bool JNILfMetastore::isTableExist(const String & db_name, const String & table_name)
{
    return jni_client->isTableExist(getOriginalDatabaseName(db_name), table_name);
}


std::vector<ApacheHive::Partition> JNILfMetastore::getPartitionsByFilter(
    const String & db_name, const String & table_name, const String & filter)
{
    String partitions_binary = jni_client->getPartitionsByFilter(
        getOriginalDatabaseName(db_name), table_name, filter, INT16_MAX);
    auto transport = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t *>(partitions_binary.data()), partitions_binary.size());
    TCompactProtocolT<TMemoryBuffer> protocal(transport);
    ApacheHive::PartitionListComposingSpec partitions_spec;
    partitions_spec.read(&protocal);
    for (auto & partition : partitions_spec.partitions)
    {
        partition.sd.location = convertTOSToS3(partition.sd.location);
    }
    return partitions_spec.partitions;
}



JNILfMetastoreClientFactory& JNILfMetastoreClientFactory::instance()
{
    static JNILfMetastoreClientFactory factory;
    return factory;
}

JNILfMetastoreClientPtr JNILfMetastoreClientFactory::getOrCreate(const String & name, const std::shared_ptr<CnchHiveSettings> & settings)
{
    auto settings_hash =  std::hash<std::string>{}(settings->toString());
    auto key = name + "#" + std::to_string(settings_hash);
    std::lock_guard<std::mutex> lock(mutex);
    auto it = clients.find(key);

    if (it != clients.end())
    {
        return it->second;
    }
    auto properties = JNILfMetastore::buildPropertiesFromHiveSettigns(*settings);
    auto client = std::make_shared<JNILfMetastore>(properties);
    clients.emplace( key, client);
    return client;
}

}

#endif
