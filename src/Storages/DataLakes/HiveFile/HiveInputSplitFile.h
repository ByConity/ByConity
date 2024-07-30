#pragma once
#include "Common/config.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace DB
{
namespace Protos { class InputSplit; }

class HiveInputSplitFile : public IHiveFile
{
public:
    HiveInputSplitFile() = default;

    HiveInputSplitFile(
        const Protos::InputSplit & input_split,
        const std::unordered_map<String, String> & properties);

    ~HiveInputSplitFile() override;
    void serialize(Protos::ProtoHiveFile & proto) const override;
    void deserialize(const Protos::ProtoHiveFile & proto) override;

    SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & params) override;

private:
    String input_split_bytes;
    std::unordered_map<String, String> properties;
};

}
#endif
