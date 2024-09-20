#include "PaimonJNIScanInfo.h"

#if USE_JAVA_EXTENSIONS

#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/PaimonCommon.h>
#include <Storages/DataLakes/Source/JNIArrowSource.h>

static constexpr auto PAIMON_CLASS_FACTORY_CLASS = "org/byconity/paimon/PaimonClassFactory";
static constexpr auto PAIMON_ARROW_READER_CLASS = "org/byconity/paimon/reader/PaimonArrowReaderBuilder";

namespace DB
{
PaimonJNIScanInfo::PaimonJNIScanInfo(
    const size_t distribution_id_, String encoded_table_, std::optional<String> encoded_predicate_, std::vector<String> encoded_splits_)
    : JNIScanInfo(ILakeScanInfo::StorageType::Paimon, distribution_id_)
    , encoded_table(std::move(encoded_table_))
    , encoded_predicate(std::move(encoded_predicate_))
    , encoded_splits(std::move(encoded_splits_))
{
}

String PaimonJNIScanInfo::identifier() const
{
    if (lazy_identifier.has_value())
        return lazy_identifier.value();

    std::lock_guard<std::mutex> lock(lazy_identifier_mutex);
    if (lazy_identifier.has_value())
        return lazy_identifier.value();

    lazy_identifier = md5(fmt::format("{}", fmt::join(encoded_splits, ",")));

    return lazy_identifier.value();
}

SourcePtr PaimonJNIScanInfo::getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params)
{
    Poco::JSON::Object json;
    json.set(paimon_utils::PARAMS_KEY_ENCODED_TABLE, encoded_table);
    json.set(paimon_utils::PARAMS_KEY_FETCH_SIZE, read_params->max_block_size);
    json.set(paimon_utils::PARAMS_KEY_REQUIRED_FIELDS, paimon_utils::concat(block.getNames()));
    json.set(paimon_utils::PARAMS_KEY_ENCODED_SPLITS, encoded_splits);
    if (encoded_predicate.has_value())
        json.set(paimon_utils::PARAMS_KEY_ENCODED_PREDICATE, encoded_predicate.value());
    auto reader
        = std::make_unique<JNIArrowReader>(PAIMON_CLASS_FACTORY_CLASS, PAIMON_ARROW_READER_CLASS, paimon_utils::serializeJson(json));
    return std::make_shared<JNIArrowSource>(block, std::move(reader));
}

void PaimonJNIScanInfo::serialize(Protos::LakeScanInfo & proto) const
{
    JNIScanInfo::serialize(proto);

    auto * paimon_jni_scan_info = proto.mutable_paimon_jni_scan_info();
    paimon_jni_scan_info->set_encoded_table(encoded_table);
    if (encoded_predicate.has_value())
        paimon_jni_scan_info->set_encoded_predicate(encoded_predicate.value());
    for (const auto & split : encoded_splits)
    {
        paimon_jni_scan_info->add_encoded_splits(split);
    }
}
LakeScanInfoPtr PaimonJNIScanInfo::deserialize(
    const Protos::LakeScanInfo & proto,
    const ContextPtr & /*context*/,
    const StorageMetadataPtr & /*metadata*/,
    const CnchHiveSettings & /*settings*/)
{
    const auto & paimon_jni_scan_info = proto.paimon_jni_scan_info();
    String encoded_table = paimon_jni_scan_info.encoded_table();
    std::optional<String> encoded_predicate;
    if (paimon_jni_scan_info.has_encoded_predicate())
    {
        encoded_predicate = paimon_jni_scan_info.encoded_predicate();
    }
    std::vector<String> encoded_splits(paimon_jni_scan_info.encoded_splits().begin(), paimon_jni_scan_info.encoded_splits().end());

    return PaimonJNIScanInfo::create(0, std::move(encoded_table), std::move(encoded_predicate), std::move(encoded_splits));
}
}

#endif
