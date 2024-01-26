#include "Storages/Hive/Metastore/JNIHiveMetastore.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/DataLakes/HiveFile/HiveInputSplitFile.h"
#include "Storages/Hive/HivePartition.h"

#include <jni/JNIMetaClient.h>
#include <hive_metastore_types.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <hudi.pb.h>

namespace DB
{

using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::protocol::TCompactProtocolT;

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

std::vector<ApacheHive::Partition>
JNIHiveMetastoreClient::getPartitionsByFilter(const String &, const String &, const String & /*filter*/)
{
    // TODO: filter
    String partitions_binary = jni_metaclient->getPartitionPaths();
    auto transport = std::make_shared<TMemoryBuffer>(reinterpret_cast<uint8_t *>(partitions_binary.data()), partitions_binary.size());
    TCompactProtocolT<TMemoryBuffer> protocal(transport);
    ApacheHive::PartitionListComposingSpec partitions_spec;
    partitions_spec.read(&protocal);
    return partitions_spec.partitions;
}

HiveFiles JNIHiveMetastoreClient::getFilesInPartition(const HivePartitions & partitions, size_t min_split_num, size_t max_threads)
{
    Protos::PartitionPaths required_partitions;
    required_partitions.set_split_num(min_split_num);
    required_partitions.set_max_threads(max_threads);
    for (const auto & partition : partitions)
        required_partitions.add_paths(partition->location);

    String input_splits_bytes = jni_metaclient->getFilesInPartition(required_partitions.SerializeAsString());
    Protos::InputSplits input_splits;
    HiveFiles hive_files;
    input_splits.ParseFromString(input_splits_bytes);
    for (const auto & input_split : input_splits.input_splits()) {
        auto it = hive_files.emplace_back(std::make_shared<HiveInputSplitFile>(input_split, properties));
        it->format = IHiveFile::FileFormat::InputSplit;
        it->partition = partitions.at(input_split.partition_index());
    }
    return hive_files;
}

}
#endif
