#include "Storages/DataLakes/HiveFile/HiveInputSplitFile.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/DataLakes/HiveFile/JNIArrowSource.h"
#include "jni/JNIArrowReader.h"
#include <Protos/lake_models.pb.h>
#include <hudi.pb.h>

namespace DB
{

static constexpr auto LAS_CLASS_FACTORY_CLASS = "org/byconity/las/LasClassFactory";
static constexpr auto INPUT_SPLIT_ARROW_READER_CLASS = "org/byconity/las/reader/LasInputSplitArrowReaderBuilder";

HiveInputSplitFile::HiveInputSplitFile(
    const Protos::InputSplit & input_split,
    const std::unordered_map<String, String> & properties_)
    : input_split_bytes(input_split.input_split_bytes()), properties(properties_)
{
}

HiveInputSplitFile::~HiveInputSplitFile() = default;

void HiveInputSplitFile::serialize(Protos::ProtoHiveFile &proto) const
{
    IHiveFile::serialize(proto);
    auto serialize_kv = [&](const String & key, const String & value) {
        auto * property = proto.mutable_properties()->add_properties();
        property->set_key(key);
        property->set_value(value);
    };

    for (const auto & kv : properties)
        serialize_kv(kv.first, kv.second);

    proto.set_input_split_bytes(input_split_bytes);
}

void HiveInputSplitFile::deserialize(const Protos::ProtoHiveFile &proto)
{
    IHiveFile::deserialize(proto);
    for (const auto & kv : proto.properties().properties())
        properties[kv.key()] = kv.value();

    input_split_bytes = proto.input_split_bytes();
}

SourcePtr HiveInputSplitFile::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    Protos::LasInputSplitArrowReaderBuilderParams params;
    auto * props = params.mutable_params();
    for (const auto & kv : properties)
    {
        auto * prop = props->add_properties();
        prop->set_key(kv.first);
        prop->set_value(kv.second);
    }
    {
        auto * prop = props->add_properties();
        prop->set_key("fetch_size");
        prop->set_value(std::to_string(read_params->max_block_size));
    }
    {
        auto * prop = props->add_properties();
        prop->set_key("required_fields");
        prop->set_value(fmt::format("{}", fmt::join(block.getNames(), ",")));
    }
    {
        if (input_split_bytes.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "input_split_bytes should not be empty");

        auto * input_split_proto = params.mutable_input_split();
        input_split_proto->set_input_split_bytes(input_split_bytes);
    }
    auto reader = std::make_unique<JNIArrowReader>(LAS_CLASS_FACTORY_CLASS, INPUT_SPLIT_ARROW_READER_CLASS, params.SerializeAsString());
    return std::make_shared<JNIArrowSource>(block, std::move(reader));
}

}
#endif
