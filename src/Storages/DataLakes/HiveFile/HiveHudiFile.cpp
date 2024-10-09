#include "Storages/DataLakes/HiveFile/HiveHudiFile.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Protos/lake_models.pb.h>
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Storages/DataLakes/HiveFile/JNIArrowSource.h"
#include "jni/JNIArrowReader.h"

#include <hudi.pb.h>

namespace DB
{

static constexpr auto HUDI_CLASS_FACTORY_CLASS = "org/byconity/hudi/HudiClassFactory";
static constexpr auto HUDI_ARROW_READER_CLASS = "org/byconity/hudi/reader/HudiArrowReaderBuilder";

HiveHudiFile::HiveHudiFile(
    const String & base_file_path,
    size_t base_file_size,
    const DiskPtr & disk_,
    const HivePartitionPtr & partition_,
    const Strings & delta_logs_,
    const std::unordered_map<String, String> & properties_)
    : properties(properties_)
{
    load(IHiveFile::FileFormat::HUDI, base_file_path, base_file_size, disk_, partition_);
    properties["data_file_path"] = file_path;
    properties["delta_file_paths"] = fmt::format("{}", fmt::join(delta_logs_, ","));
    properties["data_file_length"] = std::to_string(file_size);
}

HiveHudiFile::~HiveHudiFile() = default;

void HiveHudiFile::serialize(Protos::ProtoHiveFile & proto) const
{
    IHiveFile::serialize(proto);
    auto serialize_kv = [&](const String & key, const String & value) {
        auto * property = proto.mutable_properties()->add_properties();
        property->set_key(key);
        property->set_value(value);
    };

    for (const auto & kv : properties)
        serialize_kv(kv.first, kv.second);
}

void HiveHudiFile::deserialize(const Protos::ProtoHiveFile & proto)
{
    IHiveFile::deserialize(proto);
    for (const auto & kv : proto.properties().properties())
        properties[kv.key()] = kv.value();
}

SourcePtr HiveHudiFile::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    Protos::Properties proto;
    for (const auto & kv : properties)
    {
        auto * prop = proto.add_properties();
        prop->set_key(kv.first);
        prop->set_value(kv.second);
    }
    {
        auto * prop = proto.add_properties();
        prop->set_key("fetch_size");
        prop->set_value(std::to_string(read_params->max_block_size));
    }
    {
        auto * prop = proto.add_properties();
        prop->set_key("required_fields");
        prop->set_value(fmt::format("{}", fmt::join(block.getNames(), ",")));
    }
    auto reader = std::make_unique<JNIArrowReader>(HUDI_CLASS_FACTORY_CLASS, HUDI_ARROW_READER_CLASS, proto.SerializeAsString());
    return std::make_shared<JNIArrowSource>(block, std::move(reader));
}

}
#endif
