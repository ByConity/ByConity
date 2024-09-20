#pragma once

#include <Common/config.h>
#if USE_JAVA_EXTENSIONS

#include <mutex>
#include <Storages/DataLakes/ScanInfo/JNIScanInfo.h>

namespace DB
{
class HudiJNIScanInfo : public JNIScanInfo, public shared_ptr_helper<HudiJNIScanInfo>

{
public:
    HudiJNIScanInfo(
        size_t distribution_id_,
        const String & partition_id_,
        std::optional<String> input_split_bytes_,
        std::unordered_map<String, String> properties_);

    String identifier() const override;

    virtual SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params) override;

    virtual void serialize(Protos::LakeScanInfo & proto) const override;

    static LakeScanInfoPtr deserialize(
        const Protos::LakeScanInfo & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);

private:
    SourcePtr getReaderForLas(const Block & block, const std::shared_ptr<ReadParams> & read_params);

    const std::optional<String> input_split_bytes;
    mutable std::unordered_map<String, String> properties;

    mutable std::optional<String> lazy_identifier;
    mutable std::mutex lazy_identifier_mutex;
};
}

#endif
