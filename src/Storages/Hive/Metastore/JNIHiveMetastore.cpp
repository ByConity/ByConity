#include "JNIHiveMetastore.h"

#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <hive_metastore_types.h>
#include <Storages/DataLakes/ScanInfo/HudiJNIScanInfo.h>
#include <Storages/Hive/HivePartition.h>
#include <jni/JNIMetaClient.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <hudi.pb.h>

namespace DB
{

using apache::thrift::protocol::TCompactProtocolT;
using apache::thrift::transport::TMemoryBuffer;

JNIHiveMetastoreClient::JNIHiveMetastoreClient(
    std::shared_ptr<JNIMetaClient> jni_metaclient_, const std::unordered_map<String, String> & properties_)
    : jni_metaclient(std::move(jni_metaclient_)), properties(properties_)
{
}

std::shared_ptr<ApacheHive::Table> JNIHiveMetastoreClient::getTable(const String &, const String &)
{
    /// TODO: check whether the database and table matched
    String table_raw = jni_metaclient->getTable();
    auto transport = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t *>(table_raw.data()), table_raw.size());
    TCompactProtocolT<TMemoryBuffer> protocal(transport);
    auto table = std::make_shared<ApacheHive::Table>();
    table->read(&protocal);
    return table;
}

bool JNIHiveMetastoreClient::isTableExist(const String &, const String &)
{
    // what to do here ?
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JNIHiveMetastoreClient::isTableExists is not implemented");
}

std::vector<ApacheHive::Partition> JNIHiveMetastoreClient::getPartitionsByFilter(const String &, const String &, const String & /*filter*/)
{
    // TODO: filter
    String partitions_binary = jni_metaclient->getPartitionPaths();
    auto transport = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t *>(partitions_binary.data()), partitions_binary.size());
    TCompactProtocolT<TMemoryBuffer> protocal(transport);
    ApacheHive::PartitionListComposingSpec partitions_spec;
    partitions_spec.read(&protocal);
    return partitions_spec.partitions;
}

LakeScanInfos JNIHiveMetastoreClient::getFilesInPartition(const HivePartitions & partitions, size_t min_split_num, size_t max_threads)
{
    Protos::PartitionPaths required_partitions;
    required_partitions.set_split_num(min_split_num);
    required_partitions.set_max_threads(max_threads);
    for (const auto & partition : partitions)
        required_partitions.add_paths(partition->location);

    String input_splits_bytes = jni_metaclient->getFilesInPartition(required_partitions.SerializeAsString());
    Protos::InputSplits input_splits;
    LakeScanInfos lake_scan_infos;
    input_splits.ParseFromString(input_splits_bytes);
    size_t id = 0;
    for (const auto & input_split : input_splits.input_splits())
    {
        String partition_id = partitions.at(input_split.partition_index())->partition_id;
        lake_scan_infos.push_back(HudiJNIScanInfo::create(id++, partition_id, input_split.input_split_bytes(), properties));
    }
    return lake_scan_infos;
}

}
#endif
