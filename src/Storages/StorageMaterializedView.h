/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Common/Logger.h>
#include <common/shared_ptr_helper.h>
#include <Parsers/IAST_fwd.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/PartitionTransformer.h>
#include <Storages/MaterializedView/MaterializedViewVersionedPartCache.h>

namespace DB
{

class StorageMaterializedView final : public shared_ptr_helper<StorageMaterializedView>, public IStorage, WithMutableContext
{
    friend struct shared_ptr_helper<StorageMaterializedView>;
public:
    std::string getName() const override { return "MaterializedView"; }
    bool isView() const override { return true; }

    bool hasInnerTable() const { return has_inner_table; }

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool supportsParallelInsert(ContextPtr local_context) const override { return getTargetTable()->supportsParallelInsert(local_context); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        auto target_table = getTargetTable();
        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        return target_table->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void drop() override;
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, TableLockHolder & table_lock_holder) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr context, const ASTPtr & query = nullptr) override;

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void shutdown() override;

    virtual bool supportsOptimizer() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr & , SelectQueryInfo &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;
    StorageID getTargetTableId() const { return target_table_id; }

    String getTargetDatabaseName() const { return target_table_id.database_name;  }
    String getTargetTableName() const { return target_table_id.table_name;  }
    std::unordered_map<StorageID, BaseTableInfoPtr> getDependBaseTables();

    ActionLock getActionLock(StorageActionBlockType type) override;

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

    Strings getDataPaths() const override;

    ASTPtr getInnerQuery() const { return getInMemoryMetadataPtr()->select.inner_query->clone(); }
    bool isRefreshable(bool cascading) const;
    void refresh(const ASTPtr & partition, ContextMutablePtr local_context, bool async);
    void refreshWhere(ASTPtr partition_expr, ContextMutablePtr local_context, bool async);
    void refreshAsync(AsyncRefreshParamPtr param, ContextMutablePtr local_context) { refreshCnchAsyncImpl(param, local_context); }

    /// get async refrsh params
    AsyncRefreshParamPtrs getAsyncRefreshParams(ContextMutablePtr local_context, bool combine_params);
    void validatePartitionBased(ContextMutablePtr local_context);
    
    void validateAndSyncBaseTablePartitions(
        PartitionDiffPtr & partition_diff,
        VersionPartContainerPtrs & latest_versioned_partitions,
        ContextMutablePtr local_context,
        bool for_rewrite = false);

    void syncBaseTablePartitions(
        PartitionDiffPtr & partition_diff,
        VersionPartContainerPtrs & latest_versioned_partitions,
        const std::unordered_set<StoragePtr> & depend_base_tables,
        const std::unordered_set<StorageID> & non_depend_base_tables,
        ContextMutablePtr local_context,
        bool for_rewrite = false);
    String versionPartitionToString(const VersionPart & part);
    RefreshSchedule & getRefreshSchedule() { return refresh_schedule; }
    PartitionTransformerPtr getPartitionTransformer() const { return partition_transformer; }

    /// drop materialized view metadata 
    void dropMvMeta(ContextMutablePtr local_context);

    bool async() { return refresh_schedule.async(); }
    bool sync() { return !async(); }
    UInt64 checkAndCalRefreshSeconds() const { return refresh_schedule.prescribeNextElaps(); }
    
private:
    bool checkPartitionExpr(StoragePtr target_table, ASTPtr partition_expr, ContextMutablePtr local_context);

    void refreshLocalImpl(const ASTPtr & partition, ContextPtr local_context);

    void refreshCnchSyncImpl(const ASTPtr & partition, ContextMutablePtr local_context);

    void refreshCnchAsyncImpl(AsyncRefreshParamPtr param, ContextMutablePtr local_context);

    void executeByDropInsert(AsyncRefreshParamPtr param, ContextMutablePtr local_context);

    void executeByInsertOverwrite(AsyncRefreshParamPtr param, ContextMutablePtr local_context);

    VersionPartContainerPtrs getPreviousPartitions(ContextMutablePtr local_context);

    void insertRefreshTaskLog(AsyncRefreshParamPtr param, RefreshViewTaskStatus status,
                            bool is_insert_overwrite, std::chrono::time_point<std::chrono::system_clock> start_time,
                            ContextMutablePtr local_context, String exception = {});

    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();
    bool has_inner_table = false;
    void checkStatementCanBeForwarded() const;
    
    // Async refresh task
    RefreshSchedule refresh_schedule;
    PartitionTransformerPtr partition_transformer;

    // mv meta cache
    MaterializedViewVersionedPartCache & cache;

    LoggerPtr log;

protected:
    StorageMaterializedView(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);
};

}
