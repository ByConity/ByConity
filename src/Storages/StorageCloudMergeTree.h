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

#include <common/shared_ptr_helper.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeCloudData.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MutationCommands.h>

namespace DB
{

class CloudMergeTreeDedupWorker;
using CloudMergeTreeDedupWorkerPtr = std::unique_ptr<CloudMergeTreeDedupWorker>;
namespace IngestColumnCnch
{
    struct IngestPartitionParam;
}

class StorageCloudMergeTree : public shared_ptr_helper<StorageCloudMergeTree>, public MergeTreeCloudData
{
    friend struct shared_ptr_helper<StorageCloudMergeTree>;
    friend class CloudMergeTreeBlockOutputStream;

public:
    virtual ~StorageCloudMergeTree() override;

    std::string getName() const override { return "Cloud" + merging_params.getModeName() + "MergeTree"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsMapImplicitColumn() const override { return true; }
    bool supportIntermedicateResultCache() const override
    {
        return true;
    }

    StoragePolicyPtr getStoragePolicy(StorageLocation location) const override;
    const String& getRelativeDataPath(StorageLocation location) const override;

    void startup() override;
    void shutdown() override;
    void drop() override {}

    const auto & getCnchDatabase() const { return cnch_database_name; }
    const auto & getCnchTable() const { return cnch_table_name; }
    StorageID getCnchStorageID() const override { return StorageID(cnch_database_name, cnch_table_name, getCnchStorageUUID()); }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    ManipulationTaskPtr manipulate(const ManipulationTaskParams & params, ContextPtr task_context) override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    Pipe alterPartition(
        const StorageMetadataPtr & /* metadata_snapshot */,
        const PartitionCommands & /* commands */,
        ContextPtr /* context */,
        const ASTPtr & query = nullptr) override;

    Pipe ingestPartition(const StorageMetadataPtr &, const PartitionCommand & command, ContextPtr local_context);

    std::set<Int64> getRequiredBucketNumbers() const { return required_bucket_numbers; }
    void setRequiredBucketNumbers(std::set<Int64> & required_bucket_numbers_) { required_bucket_numbers = required_bucket_numbers_; }
    ASTs convertBucketNumbersToAstLiterals(const ASTPtr where_expression, ContextPtr context) const;

    /// check whether staged parts are too old.
    bool checkStagedParts();

    CloudMergeTreeDedupWorker * tryGetDedupWorker() { return dedup_worker.get(); }
    CloudMergeTreeDedupWorker * getDedupWorker();

    QueryProcessingStage::Enum getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;
    bool getQueryProcessingStageWithAggregateProjection(ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const;

    void resetObjectColumns(const ColumnsDescription & object_columns_) { object_columns = object_columns_; }

protected:
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    StorageCloudMergeTree(
        const StorageID & table_id_,
        String cnch_database_name_,
        String cnch_table_name_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);


    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    const String cnch_database_name;
    const String cnch_table_name;

private:

    // Relative path to auxility storage disk root
    String relative_auxility_storage_path;

    std::set<Int64> required_bucket_numbers;

    CloudMergeTreeDedupWorkerPtr dedup_worker;
};

}
