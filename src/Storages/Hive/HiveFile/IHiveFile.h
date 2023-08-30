#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Core/NamesAndTypes.h"
#include "Disks/IDisk.h"
#include "Processors/ISource.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Storages/MergeTree/KeyCondition.h"
#include "Storages/KeyDescription.h"

namespace DB
{

namespace Protos
{
    class ProtoHiveFile;
    class ProtoHiveFiles;
}

class IHiveFile
{
public:
    friend struct HiveFilesSerDe;
    enum class FileFormat
    {
        PARQUET,
        ORC,
    };
    static FileFormat fromHdfsInputFormatClass(const String & class_name);
    static FileFormat fromFormatName(const String & format_name);
    static String toString(FileFormat format);

    /// a very basic factory method
    static HiveFilePtr create(
        FileFormat format,
        String file_path,
        size_t file_size,
        const DiskPtr & disk,
        const HivePartitionPtr & partition);

    virtual ~IHiveFile() = default;

    virtual void serialize(Protos::ProtoHiveFile & proto) const;

    struct Features
    {
        bool support_file_splits = false;
        bool support_file_minmax_index = false;
        bool support_split_minmax_index = false;
    };
    virtual Features getFeatures() const = 0;

    FileFormat getFormat() const { return format; }
    String getFormatName() const;
    std::unique_ptr<ReadBufferFromFileBase> readFile(const ReadSettings & settings = {}) const;

    virtual size_t numSlices() const { return 1; }
    virtual std::optional<size_t> numRows() const { return {}; }

    struct MinMaxIndex
    {
        std::vector<Range> hyperrectangle;
        bool initialized = false;
    };
    using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;
    MinMaxIndexPtr getMinMaxIndex() const { return file_minmax_idx; }
    const std::vector<MinMaxIndexPtr> & getSplitMinMaxIndex() const { return split_minmax_idxes; }

    /// TODO: file min max
    /// virtual void loadFileMinMaxIndex(const NamesAndTypesList & index_names_and_types) = 0;
    virtual void loadSplitMinMaxIndex(const NamesAndTypesList & index_names_and_types) = 0;
    String describeMinMaxIndex(const NamesAndTypesList & index_names_and_types) const;
    void setSkipSplits(const std::vector<bool> & skip_splits_) { skip_splits = skip_splits_; }
    bool canSkipSplit(size_t split) { return !skip_splits.empty() && skip_splits.at(split); }

    struct ReadParams
    {
        size_t max_block_size;
        FormatSettings format_settings;
        ContextPtr context;
        std::optional<size_t> slice;
        ReadSettings read_settings;
        std::unique_ptr<ReadBufferFromFileBase> read_buf;
    };
    virtual SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & params);

    FileFormat format;
    String file_path;
    size_t file_size;

    DiskPtr disk;
    HivePartitionPtr partition;
    std::vector<bool> skip_splits; // tricky

protected:
    IHiveFile() = default;

    MinMaxIndexPtr file_minmax_idx;
    std::vector<MinMaxIndexPtr> split_minmax_idxes;
};

using HiveFilePtr = std::shared_ptr<IHiveFile>;
using HiveFiles = std::vector<HiveFilePtr>;

namespace RPCHelpers
{
    void serialize(Protos::ProtoHiveFiles & proto, const HiveFiles & hive_files);
    HiveFiles deserialize(
        const Protos::ProtoHiveFiles & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);
}

}

#endif
