#pragma once

#include "Core/Field.h"
#include "Storages/StorageInMemoryMetadata.h"

namespace DB
{
class JNIMetaClient;

// struct HudiPartition
// {
//     Row value;

//     void load(const StorageInMemoryMetadata & storage, const String & partition);

// private:
//     Strings parse(const String & partition);
// };

struct RemoteFileInfo
{
    String base_file_path;
    size_t base_file_length;
    String instant;
    Strings delta_logs;
    String format {"Parquet"}; // only support parquet now

    String toString() const;
};

using RemoteFileInfos = std::vector<RemoteFileInfo>;

RemoteFileInfos huidListDirectory(JNIMetaClient & client, const String & base_path, const String & partition_path);

}
