#pragma once

#include <ctime>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <WorkerTasks/CnchMergePrefetcher.h>

namespace DB
{

class MergeTreePrefetchedReaderCNCH : public MergeTreeReaderWide
{
public:
    MergeTreePrefetchedReaderCNCH(
        const MergeTreeMetaBase::DataPartPtr& data_part_,
        const NamesAndTypesList& columns_,
        const StorageMetadataPtr& metadata_snapshot_,
        MarkCache* mark_cache_,
        const MarkRanges& mark_ranges_,
        const MergeTreeReaderSettings& settings_,
        CnchMergePrefetcher::PartFutureFiles* future_files_,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback_ = ReadBufferFromFile::ProfileCallback{},
        clockid_t clock_type = CLOCK_MONOTONIC_COARSE);

    virtual ~MergeTreePrefetchedReaderCNCH() override;

protected:
    void addStreams(const NameAndTypePair& name_and_type,
        ReadBufferFromFileBase::ProfileCallback profile_callback, clockid_t clock_type,
        MergeTreeIndexGranularityInfo* mocked_index_granularity_info);

    CnchMergePrefetcher::PartFutureFiles * future_files;
};

}
