#include "Storages/DataLakes/HudiMorDirectoryLister.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <jni/JNIMetaClient.h>
#include <Protos/lake_models.pb.h>
#include "Common/StringUtils/StringUtils.h"
#include "Storages/DataLakes/HiveFile/HiveHudiFile.h"
#include "Storages/DataLakes/StorageCnchHudi.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HivePartition.h"

#include "hudi.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr auto HUDI_CLASS_FACTORY_CLASS = "org/byconity/hudi/HudiClassFactory";
static constexpr auto HUDI_CLIENT_CLASS = "org/byconity/hudi/HudiMetaClient";

static String getRelativePartitionPath(const String & _base_path, const String & _partition_path)
{
    String base_path = HiveUtil::getPath(_base_path);
    String partition_path = HiveUtil::getPath(_partition_path);
    if (!startsWith(partition_path, base_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition path {} does not belong to base path {}", partition_path, base_path);
    }
    else
    {
        return std::filesystem::relative(partition_path, base_path);
    }
}

HudiMorDirectoryLister::HudiMorDirectoryLister(const DiskPtr & disk_, const String & base_path, const StorageCnchHudi & hudi_table)
    : DiskDirectoryLister(disk_, IHiveFile::FileFormat::HUDI)
{
    table_properties["input_format"] = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    table_properties["serde"] = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    table_properties["hive_column_names"] = fmt::format("{}", fmt::join(hudi_table.getHiveColumnNames(), ","));
    table_properties["hive_column_types"] = fmt::format("{}", fmt::join(hudi_table.getHiveColumnTypes(), "#"));
    table_properties["base_path"] = base_path;

    Protos::HudiMetaClientParams params;
    auto add_kv = [&](const char * key, const String & value) {
        auto * property = params.mutable_properties()->add_properties();
        property->set_key(key);
        property->set_value(value);
    };
    add_kv("base_path", basePath());
    params.PrintDebugString();
    jni_client = std::make_shared<JNIMetaClient>(HUDI_CLASS_FACTORY_CLASS, HUDI_CLIENT_CLASS, params.SerializeAsString());
}

HiveFiles HudiMorDirectoryLister::list(const HivePartitionPtr & partition)
{
    String files = jni_client->getFilesInPartition(getRelativePartitionPath(basePath(), partition->location));
    Protos::HudiFileSlices file_slices;
    file_slices.ParseFromString(files);
    std::filesystem::path full_path(partition->location);

    HiveFiles hudi_files;
    hudi_files.reserve(file_slices.file_slices_size());

    auto properties = table_properties;
    properties["instant_time"] = file_slices.instant();

    for (const auto & file_slice : file_slices.file_slices())
    {
        Strings delta_logs;
        delta_logs.reserve(file_slice.delta_logs_size());
        std::transform(
            file_slice.delta_logs().begin(), file_slice.delta_logs().end(), std::back_inserter(delta_logs), [&](const auto & delta_log) {
                return full_path / delta_log;
            });
        auto hudi_file = std::make_shared<HiveHudiFile>(
            full_path / file_slice.base_file_name(), file_slice.base_file_length(), disk, partition, delta_logs, properties);
        hudi_files.push_back(std::move(hudi_file));
    }
    return hudi_files;
}

const String & HudiMorDirectoryLister::basePath()
{
    return table_properties.at("base_path");
}

}
#endif
