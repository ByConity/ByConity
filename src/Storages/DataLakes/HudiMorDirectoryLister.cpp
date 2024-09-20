#include "Storages/DataLakes/HudiMorDirectoryLister.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/ScanInfo/HudiJNIScanInfo.h>
#include <Storages/DataLakes/StorageCnchHudi.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/DirectoryLister.h>
#include <Storages/Hive/HivePartition.h>
#include <jni/JNIMetaClient.h>
#include <Common/StringUtils/StringUtils.h>

#include "hudi.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr auto HUDI_CLASS_FACTORY_CLASS = "org/byconity/hudi/HudiClassFactory";
static constexpr auto HUDI_CLIENT_CLASS = "org/byconity/hudi/HudiMetaClient";

HudiMorDirectoryLister::HudiMorDirectoryLister(const String & base_path, const StorageCnchHudi & hudi_table)
{
    table_properties["input_format"] = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    table_properties["serde"] = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    table_properties["hive_column_names"] = fmt::format("{}", fmt::join(hudi_table.getHiveColumnNames(), ","));
    table_properties["hive_column_types"] = fmt::format("{}", fmt::join(hudi_table.getHiveColumnTypes(), "#"));
    table_properties["base_path"] = base_path;

    // S3 properties
    if (!hudi_table.getSettings()->endpoint.value.empty() || !hudi_table.getSettings()->ak_id.value.empty()
        || !hudi_table.getSettings()->ak_secret.value.empty())
    {
        table_properties["fs.s3.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem";
        table_properties["fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem";
        table_properties["fs.s3a.endpoint"] = hudi_table.getSettings()->endpoint.value;
        table_properties["fs.s3a.access.key"] = hudi_table.getSettings()->ak_id.value;
        table_properties["fs.s3a.secret.key"] = hudi_table.getSettings()->ak_secret.value;
        table_properties["fs.s3a.path.style.access"] = std::to_string(hudi_table.getSettings()->s3_use_virtual_hosted_style.value);
        if (!hudi_table.getSettings()->s3_extra_options.value.empty())
        {
            std::vector<String> s3_extra_option_pairs = CnchHiveSettings::splitStr(hudi_table.getSettings()->s3_extra_options.value, ",");
            for (const auto & pair : s3_extra_option_pairs)
            {
                auto kv = CnchHiveSettings::splitStr(pair, "=");
                if (kv.size() != 2)
                    throw Exception("Invalid s3 extra option: " + pair, ErrorCodes::BAD_ARGUMENTS);
                table_properties[kv[0].c_str()] = kv[1];
            }
        }
    }
    Protos::HudiMetaClientParams params;
    for (const auto & [key, value] : table_properties)
    {
        auto * property = params.mutable_properties()->add_properties();
        property->set_key(key);
        property->set_value(value);
    }
    jni_client = std::make_shared<JNIMetaClient>(HUDI_CLASS_FACTORY_CLASS, HUDI_CLIENT_CLASS, params.SerializeAsString());
}

LakeScanInfos HudiMorDirectoryLister::list(const HivePartitionPtr & partition)
{
    Protos::PartitionPaths required_partitions;
    required_partitions.add_paths(partition->location);
    String files = jni_client->getFilesInPartition(required_partitions.SerializeAsString());
    Protos::HudiFileSlices file_slices;
    file_slices.ParseFromString(files);
    std::filesystem::path full_path(partition->location);

    LakeScanInfos lake_scan_infos;
    lake_scan_infos.reserve(file_slices.file_slices_size());


    size_t id = 0;
    for (const auto & file_slice : file_slices.file_slices())
    {
        Strings delta_logs;
        delta_logs.reserve(file_slice.delta_logs_size());
        std::transform(
            file_slice.delta_logs().begin(), file_slice.delta_logs().end(), std::back_inserter(delta_logs), [&](const auto & delta_log) {
                return full_path / delta_log;
            });

        auto properties_copy = table_properties;
        properties_copy["instant_time"] = file_slices.instant();
        properties_copy["data_file_path"] = full_path / file_slice.base_file_name();
        properties_copy["delta_file_paths"] = fmt::format("{}", fmt::join(delta_logs, ","));
        properties_copy["data_file_length"] = std::to_string(file_slice.base_file_length());
        lake_scan_infos.push_back(HudiJNIScanInfo::create(id++, partition->partition_id, std::nullopt, std::move(properties_copy)));
    }
    return lake_scan_infos;
}

const String & HudiMorDirectoryLister::basePath()
{
    return table_properties.at("base_path");
}

}
#endif
