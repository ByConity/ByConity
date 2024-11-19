#pragma once

#include <Disks/IDisk.h>
#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/HivePartition.h>
#include <Poco/URI.h>
#include <common/types.h>

namespace DB
{
class FileScanInfo : public ILakeScanInfo, public shared_ptr_helper<FileScanInfo>
{
    friend shared_ptr_helper<FileScanInfo>;

public:
    enum FormatType
    {
        PARQUET,
        ORC,

    };
    static FormatType parseFormatTypeFromString(const String & format_name);

    FileScanInfo(
        StorageType storage_type_, FormatType format_type_, const String & uri_, size_t size_, std::optional<String> partition_id_);

    String identifier() const override { return uri.getPath(); }

    SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params) override;

    void serialize(Protos::LakeScanInfo & proto) const override;

    FormatType getFormatType() const { return format_type; }
    String getPath() const { return uri.getPath(); }
    size_t getSize() const { return size; }

    static LakeScanInfoPtr deserialize(
        const Protos::LakeScanInfo & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);


private:
    // Only used for deserialize
    FileScanInfo(
        StorageType storage_type_,
        FormatType format_type_,
        const String & uri_,
        size_t size_,
        DiskPtr disk_,
        std::optional<String> partition_id_);

    std::unique_ptr<ReadBufferFromFileBase> readFile(const ReadSettings & settings = {}) const;

    const FormatType format_type;
    const Poco::URI uri;
    const size_t size;
    const DiskPtr disk;

    LoggerPtr log{getLogger("FileScanInfo")};
};
}
