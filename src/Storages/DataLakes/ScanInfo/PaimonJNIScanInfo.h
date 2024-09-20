#pragma once

#include <Common/config.h>
#if USE_JAVA_EXTENSIONS

#include <Storages/DataLakes/ScanInfo/JNIScanInfo.h>

namespace DB
{
class PaimonJNIScanInfo : public JNIScanInfo, public shared_ptr_helper<PaimonJNIScanInfo>
{
public:
    PaimonJNIScanInfo(
        size_t distribution_id_, String encoded_table_, std::optional<String> encoded_predicate_, std::vector<String> encoded_splits_);

    String identifier() const override;

    virtual SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params) override;

    virtual void serialize(Protos::LakeScanInfo & proto) const override;

    static LakeScanInfoPtr deserialize(
        const Protos::LakeScanInfo & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);

private:
    const String encoded_table;
    const std::optional<String> encoded_predicate;
    const std::vector<String> encoded_splits;

    mutable std::optional<String> lazy_identifier;
    mutable std::mutex lazy_identifier_mutex;
};
}

#endif
