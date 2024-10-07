#include "HivePaimonFile.h"

#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/PaimonCommon.h>
#include <Storages/Hive/HiveFile/IHiveFile_fwd.h>
#include <Poco/DigestStream.h>
#include <Poco/HexBinaryEncoder.h>
#include <Poco/MD5Engine.h>

static constexpr auto PAIMON_CLASS_FACTORY_CLASS = "org/byconity/paimon/PaimonClassFactory";
static constexpr auto PAIMON_ARROW_READER_CLASS = "org/byconity/paimon/reader/PaimonArrowReaderBuilder";

namespace DB
{
HivePaimonFile::HivePaimonFile(
    const String & encoded_table, const std::optional<String> & encoded_predicate, const std::vector<String> & encoded_splits)
{
    // Paimon has no path, but the IHiveFile process requires it unique for different splits, so use md5 as its path
    String path;
    {
        Poco::MD5Engine md5;
        Poco::DigestOutputStream dos(md5);
        dos << fmt::format("{}", fmt::join(encoded_splits, ","));
        dos.close();

        const Poco::DigestEngine::Digest & digest = md5.digest();
        path = Poco::DigestEngine::digestToHex(digest);
    }
    load(IHiveFile::FileFormat::Paimon, path, 0, nullptr, nullptr);
    properties.emplace(paimon_utils::PARAMS_KEY_ENCODED_TABLE, encoded_table);
    if (encoded_predicate.has_value())
    {
        properties.emplace(paimon_utils::PARAMS_KEY_ENCODED_PREDICATE, encoded_predicate.value());
    }
    properties.emplace(paimon_utils::PARAMS_KEY_ENCODED_SPLITS, paimon_utils::concat(encoded_splits));
}

void HivePaimonFile::serialize(Protos::ProtoHiveFile & proto) const
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

void HivePaimonFile::deserialize(const Protos::ProtoHiveFile & proto)
{
    IHiveFile::deserialize(proto);
    for (const auto & kv : proto.properties().properties())
        properties[kv.key()] = kv.value();
}

SourcePtr HivePaimonFile::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_ENCODED_TABLE, properties[paimon_utils::PARAMS_KEY_ENCODED_TABLE]);
    json.set(paimon_utils::PARAMS_KEY_FETCH_SIZE, read_params->max_block_size);
    json.set(paimon_utils::PARAMS_KEY_REQUIRED_FIELDS, paimon_utils::concat(block.getNames()));
    json.set(paimon_utils::PARAMS_KEY_ENCODED_SPLITS, properties[paimon_utils::PARAMS_KEY_ENCODED_SPLITS]);
    if (properties.find(paimon_utils::PARAMS_KEY_ENCODED_PREDICATE) != properties.end())
        json.set(paimon_utils::PARAMS_KEY_ENCODED_PREDICATE, properties[paimon_utils::PARAMS_KEY_ENCODED_PREDICATE]);
    auto reader
        = std::make_unique<JNIArrowReader>(PAIMON_CLASS_FACTORY_CLASS, PAIMON_ARROW_READER_CLASS, paimon_utils::serializeJson(json));
    return std::make_shared<JNIArrowSource>(block, std::move(reader));
}
}
#endif
