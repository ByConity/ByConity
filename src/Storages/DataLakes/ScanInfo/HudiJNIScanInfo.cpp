#include "HudiJNIScanInfo.h"

#if USE_JAVA_EXTENSIONS

#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/Source/JNIArrowSource.h>
#include <jni/JNIArrowReader.h>
#include <hudi.pb.h>

static constexpr auto HUDI_CLASS_FACTORY_CLASS = "org/byconity/hudi/HudiClassFactory";
static constexpr auto HUDI_ARROW_READER_CLASS = "org/byconity/hudi/reader/HudiArrowReaderBuilder";
static constexpr auto LAS_HUDI_CLASS_FACTORY_CLASS = "org/byconity/las/LasClassFactory";
static constexpr auto LAS_HUDI_ARROW_READER_CLASS = "org/byconity/las/reader/LasInputSplitArrowReaderBuilder";

namespace DB
{
HudiJNIScanInfo::HudiJNIScanInfo(
    const size_t distribution_id_,
    const String & partition_id_,
    std::optional<String> input_split_bytes_,
    std::unordered_map<String, String> properties_)
    : JNIScanInfo(ILakeScanInfo::StorageType::Hudi, distribution_id_)
    , input_split_bytes(std::move(input_split_bytes_))
    , properties(std::move(properties_))
{
    partition_id = partition_id_;
}

String HudiJNIScanInfo::identifier() const
{
    if (lazy_identifier.has_value())
        return lazy_identifier.value();

    std::lock_guard<std::mutex> lock(lazy_identifier_mutex);
    if (lazy_identifier.has_value())
        return lazy_identifier.value();

    if (input_split_bytes.has_value())
        lazy_identifier = md5(input_split_bytes.value());
    else
        lazy_identifier = md5(properties["data_file_path"]);

    return lazy_identifier.value();
}

SourcePtr HudiJNIScanInfo::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    if (input_split_bytes.has_value())
        return getReaderForLas(block, read_params);

    Protos::HudiProperties proto;
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

SourcePtr HudiJNIScanInfo::getReaderForLas(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    Protos::LasInputSplitArrowReaderBuilderParams builder_params;
    auto * props = builder_params.mutable_params();
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

    auto * input_split_proto = builder_params.mutable_input_split();
    input_split_proto->set_input_split_bytes(input_split_bytes.value());

    auto reader
        = std::make_unique<JNIArrowReader>(LAS_HUDI_CLASS_FACTORY_CLASS, LAS_HUDI_ARROW_READER_CLASS, builder_params.SerializeAsString());
    return std::make_shared<JNIArrowSource>(block, std::move(reader));
}

void HudiJNIScanInfo::serialize(Protos::LakeScanInfo & proto) const
{
    JNIScanInfo::serialize(proto);

    auto * hudi_jni_scan_info = proto.mutable_hudi_jni_scan_info();
    hudi_jni_scan_info->set_partition_id(partition_id.value());
    for (const auto & kv : properties)
    {
        auto * prop = hudi_jni_scan_info->mutable_properties()->add_properties();
        prop->set_key(kv.first);
        prop->set_value(kv.second);
    }
    if (input_split_bytes.has_value())
    {
        hudi_jni_scan_info->set_input_split_bytes(input_split_bytes.value());
    }
}

LakeScanInfoPtr HudiJNIScanInfo::deserialize(
    const Protos::LakeScanInfo & proto,
    const ContextPtr & /*context*/,
    const StorageMetadataPtr & /*metadata*/,
    const CnchHiveSettings & /*settings*/)
{
    const auto & hudi_jni_scan_info = proto.hudi_jni_scan_info();

    String partition_id = hudi_jni_scan_info.partition_id();
    std::unordered_map<String, String> properties;
    for (const auto & prop : hudi_jni_scan_info.properties().properties())
    {
        properties[prop.key()] = prop.value();
    }

    std::optional<String> input_split_bytes = std::nullopt;
    if (hudi_jni_scan_info.has_input_split_bytes())
    {
        input_split_bytes = hudi_jni_scan_info.input_split_bytes();
    }

    return HudiJNIScanInfo::create(0, partition_id, std::move(input_split_bytes), std::move(properties));
}
}

#endif
