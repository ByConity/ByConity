#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_);

    using WrittenOffsetColumns = std::set<std::string>;

    const MergeTreeIndexGranularity & getIndexGranularity()
    {
        return writer->getIndexGranularity();
    }

    virtual void updateWriterStream(const NameAndTypePair &pair);

protected:
    // using SerializationState = ISerialization::SerializeBinaryBulkStatePtr;

    // ISerialization::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns);

    /// Remove all columns marked expired in data_part. Also, clears checksums
    /// and columns array. Return set of removed files names.
    static NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        MergeTreeMetaBase::DataPart::Checksums & checksums);

protected:
    const MergeTreeMetaBase & storage;
    StorageMetadataPtr metadata_snapshot;

    VolumePtr volume;
    String part_path;

    IMergeTreeDataPart::MergeTreeWriterPtr writer;
};

}
