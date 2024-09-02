#pragma once
#include "Common/config.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace DB
{
class HiveHudiFile : public IHiveFile
{
public:
    HiveHudiFile() = default;

    HiveHudiFile(
        const String & base_file_path,
        size_t base_file_size,
        const DiskPtr & disk,
        const HivePartitionPtr & partition,
        const Strings & delta_logs,
        const std::unordered_map<String, String> & properties);

    ~HiveHudiFile() override;
    void serialize(Protos::ProtoHiveFile & proto) const override;
    void deserialize(const Protos::ProtoHiveFile & proto) override;

    SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & params) override;
private:
    std::unordered_map<String, String> properties;
};

}
#endif
