#include "Storages/DataLakes/HudiPartition.h"
#include <algorithm>
#include <fmt/format.h>
#include <hudi.pb.h>
#include "JNI/JNIMetaClient.h"
#include "Storages/StorageInMemoryMetadata.h"

namespace DB
{

// void HudiPartition::load(const DB::StorageInMemoryMetadata, const String & partition)
// {
// }

// Strings HudiPartition::parse(const String & partition)
// {
// }

String RemoteFileInfo::toString() const
{
    return fmt::format(
        "base_file: {}, delta: {}, format: {}", base_file_path, fmt::join(delta_logs, ", "), format);
}

RemoteFileInfos huidListDirectory(JNIMetaClient & client, const String & base_path, const String & partition_path)
{
    String slices = client.getFilesInPartition(partition_path);
    Protos::HudiFileSlices file_slices;
    file_slices.ParseFromString(slices);
    RemoteFileInfos remote_files;
    remote_files.reserve(file_slices.file_slices_size());

    auto full_path = std::filesystem::path(base_path) / partition_path;
    for (const auto & file_slice : file_slices.file_slices())
    {
        Strings delta_logs;
        delta_logs.reserve(file_slice.delta_logs_size());
        std::transform(
            file_slice.delta_logs().begin(), file_slice.delta_logs().end(), std::back_inserter(delta_logs), [&](const auto & delta_log) {
                return full_path / delta_log;
            });

        RemoteFileInfo remote_file{
            .base_file_path = full_path / file_slice.base_file_name(),
            .base_file_length = file_slice.base_file_length(),
            .instant = file_slices.instant(),
            .delta_logs = std::move(delta_logs)};
        remote_files.emplace_back(std::move(remote_file));
    }
    return remote_files;
}
}
