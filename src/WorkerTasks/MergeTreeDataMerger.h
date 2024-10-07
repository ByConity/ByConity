/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Processors/Pipe.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <WorkerTasks/ManipulationList.h>
#include <WorkerTasks/ManipulationProgress.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <common/logger_useful.h>

namespace Poco
{
class TemporaryFile;
}

namespace DB
{

struct ManipulationTaskParams;
struct ManipulationListElement;
struct MergeTreeWriterSettings;

class CompressedReadBufferFromFile;
class CnchMergePrefetcher;
class MergedBlockOutputStream;
class MergeTreeMetaBase;

class MergeTreeDataMerger
{
public:
    using CheckCancelCallback = std::function<bool()>;

    MergeTreeDataMerger(
        MergeTreeMetaBase & data_,
        const ManipulationTaskParams & params_,
        ContextPtr context_,
        ManipulationListElement * manipulation_entry_,
        CheckCancelCallback check_cancel_,
        bool build_rowid_mappings = false);

    ~MergeTreeDataMerger();

    MergeTreeMutableDataPartPtr mergePartsToTemporaryPart();

    /// The i-th input row is mapped to the RowidMapping[i]-th output row in the merged part
    /// TODO: the elements are sorted integers, could apply encoding to reduce memory footprint
    using RowidMapping = PODArray<UInt32, /*INITIAL_SIZE*/ 1024>;

    /// REQUIRES:
    /// 1. when constructing the merger, build_rowid_mappings is true
    /// 2. mergePartsToTemporaryPart() has been called
    const RowidMapping & getRowidMapping(size_t part_index) const { return rowid_mappings[part_index]; }

private:
    MergeTreeMutableDataPartPtr mergePartsToTemporaryPartImpl(
        const MergeTreeDataPartsVector & source_data_parts,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeMetaBase::MergingParams & merging_params,
        ManipulationListElement * manipulation_entry,
        const IMergeTreeDataPart * parent_part);

    void prepareColumnNamesAndTypes(
        const StorageSnapshotPtr & storage_snapshot,
        const MergeTreeMetaBase::MergingParams & merging_params,
        Names & all_column_names,
        Names & gathering_column_names,
        Names & merging_column_names,
        NamesAndTypesList & storage_columns,
        NamesAndTypesList & gathering_columns,
        NamesAndTypesList & merging_columns);
    
    MergeTreeMutableDataPartPtr prepareNewParts(
        const MergeTreeDataPartsVector & source_data_parts,
        const IMergeTreeDataPart * parent_part,
        const NamesAndTypesList & storage_columns);

    MergeAlgorithm chooseMergeAlgorithm(
        const MergeTreeDataPartsVector & source_data_parts,
        const NamesAndTypesList & gathering_columns, 
        const MergeTreeMetaBase::MergingParams & merging_params,
        size_t sum_input_rows_upper_bound);
    
private:
    MergeTreeMetaBase & data;
    MergeTreeSettingsPtr data_settings;
    const ManipulationTaskParams & params;
    ContextPtr context;
    ManipulationListElement * task_manipulation_entry;
    CheckCancelCallback check_cancel;
    bool build_rowid_mappings;
    /// rowid mapping for each input part, only for normal parts
    std::vector<RowidMapping> rowid_mappings;
    LoggerPtr log = nullptr;
    ReservationPtr space_reservation;
    /// Used for building rowid mappings
    size_t output_rowid = 0;

    /// NOTE: must be constructed before (src/merged) streams, and deconstructed after streams.
    std::unique_ptr<CnchMergePrefetcher> prefetcher;
};

} /// EOF
