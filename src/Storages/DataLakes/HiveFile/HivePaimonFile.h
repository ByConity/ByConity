#pragma once
#include <Common/config.h>
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Storages/Hive/HiveFile/IHiveFile.h>

namespace DB
{
class HivePaimonFile : public IHiveFile
{
public:
    HivePaimonFile() = default;

    HivePaimonFile(
        const String & encoded_table, const std::optional<String> & encoded_predicate, const std::vector<String> & encoded_splits);

    ~HivePaimonFile() override = default;

    void serialize(Protos::ProtoHiveFile & proto) const override;
    void deserialize(const Protos::ProtoHiveFile & proto) override;

    SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & params) override;

private:
    std::unordered_map<String, String> properties;
};
}

#endif
