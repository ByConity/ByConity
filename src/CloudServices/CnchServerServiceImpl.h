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

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#include <Common/Logger.h>
#include <MergeTreeCommon/GlobalGCManager.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <common/logger_useful.h>
#include <Common/Brpc/BrpcServiceDefines.h>

namespace DB
{

class CnchServerServiceImpl : protected WithMutableContext, public DB::Protos::CnchServerService
{
public:
    explicit CnchServerServiceImpl(ContextMutablePtr global_context);
    ~CnchServerServiceImpl() override = default;

    void reportTaskHeartbeat(
        google::protobuf::RpcController * cntl,
        const Protos::ReportTaskHeartbeatReq * request,
        Protos::ReportTaskHeartbeatResp * response,
        google::protobuf::Closure * done) override;

    void reportDeduperHeartbeat(
        google::protobuf::RpcController * cntl,
        const Protos::ReportDeduperHeartbeatReq * request,
        Protos::ReportDeduperHeartbeatResp * response,
        google::protobuf::Closure * done) override;

    void createTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::CreateTransactionReq * request,
        Protos::CreateTransactionResp * response,
        google::protobuf::Closure * done) override;

    void finishTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::FinishTransactionReq * request,
        Protos::FinishTransactionResp * response,
        google::protobuf::Closure * done) override;

    void commitTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::CommitTransactionReq * request,
        Protos::CommitTransactionResp * response,
        google::protobuf::Closure * done) override;

    void precommitTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::PrecommitTransactionReq * request,
        Protos::PrecommitTransactionResp * response,
        google::protobuf::Closure * done) override;

    void redirectCommitTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::RedirectCommitTransactionReq * request,
        Protos::RedirectCommitTransactionResp * response,
        google::protobuf::Closure * done) override;

    void rollbackTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::RollbackTransactionReq * request,
        Protos::RollbackTransactionResp * response,
        google::protobuf::Closure * done) override;

    void checkConsumerValidity(
        google::protobuf::RpcController * cntl,
        const Protos::CheckConsumerValidityReq * request,
        Protos::CheckConsumerValidityResp * response,
        google::protobuf::Closure * done) override;

    void createTransactionForKafka(
        google::protobuf::RpcController * cntl,
        const Protos::CreateKafkaTransactionReq * request,
        Protos::CreateKafkaTransactionResp * response,
        google::protobuf::Closure * done) override;

    void commitParts(
        google::protobuf::RpcController * cntl,
        const Protos::CommitPartsReq * request,
        Protos::CommitPartsResp * response,
        google::protobuf::Closure * done) override;

    void fetchDataParts(
        ::google::protobuf::RpcController * c,
        const ::DB::Protos::FetchDataPartsReq * request,
        ::DB::Protos::FetchDataPartsResp * response,
        ::google::protobuf::Closure * done) override;

    void fetchDeleteBitmaps(
        ::google::protobuf::RpcController * c,
        const ::DB::Protos::FetchDeleteBitmapsReq * request,
        ::DB::Protos::FetchDeleteBitmapsResp * response,
        ::google::protobuf::Closure * done) override;

    void fetchPartitions(
        google::protobuf::RpcController * cntl,
        const ::DB::Protos::FetchPartitionsReq* request,
        ::DB::Protos::FetchPartitionsResp* response,
        ::google::protobuf::Closure* done) override;

    void fetchUniqueTableMeta(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::FetchUniqueTableMetaReq * request,
        ::DB::Protos::FetchUniqueTableMetaResp * response,
        ::google::protobuf::Closure * done) override;

    void getMinActiveTimestamp(
        google::protobuf::RpcController * cntl,
        const Protos::GetMinActiveTimestampReq * request,
        Protos::GetMinActiveTimestampResp * response,
        google::protobuf::Closure * done) override;

    /***
     *  About CNCH background threads
     */
    void getBackgroundThreadStatus(
        google::protobuf::RpcController * cntl,
        const Protos::BackgroundThreadStatusReq * request,
        Protos::BackgroundThreadStatusResp * response,
        google::protobuf::Closure * done) override;
    void getNumBackgroundThreads(
        google::protobuf::RpcController * cntl,
        const Protos::BackgroundThreadNumReq * request,
        Protos::BackgroundThreadNumResp * response,
        google::protobuf::Closure * done) override;
    void controlCnchBGThread(
        google::protobuf::RpcController * cntl,
        const Protos::ControlCnchBGThreadReq * request,
        Protos::ControlCnchBGThreadResp * response,
        google::protobuf::Closure * done) override;

    void getTablePartitionInfo(
        google::protobuf::RpcController * cntl,
        const Protos::GetTablePartitionInfoReq * request,
        Protos::GetTablePartitionInfoResp * response,
        google::protobuf::Closure * done) override;

    void getTableInfo(
        google::protobuf::RpcController * cntl,
        const Protos::GetTableInfoReq * request,
        Protos::GetTableInfoResp * response,
        google::protobuf::Closure * done) override;

    void invalidateBytepond(
        google::protobuf::RpcController * cntl,
        const Protos::InvalidateBytepondReq * request,
        Protos::InvalidateBytepondResp * response,
        google::protobuf::Closure * done) override;

    void getTransactionStatus(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::GetTransactionStatusReq * request,
        ::DB::Protos::GetTransactionStatusResp * response,
        ::google::protobuf::Closure * done) override;

    void commitWorkerRPCByKey(
        google::protobuf::RpcController * cntl,
        const Protos::CommitWorkerRPCByKeyReq * request,
        Protos::CommitWorkerRPCByKeyResp * response,
        google::protobuf::Closure * done) override;

    void cleanTransaction(
        google::protobuf::RpcController * cntl,
        const Protos::CleanTransactionReq * request,
        Protos::CleanTransactionResp * response,
        google::protobuf::Closure * done) override;

    void cleanUndoBuffers(
        google::protobuf::RpcController * cntl,
        const Protos::CleanUndoBuffersReq * request,
        Protos::CleanUndoBuffersResp * response,
        google::protobuf::Closure * done) override;

    void acquireLock(
        google::protobuf::RpcController * cntl,
        const Protos::AcquireLockReq * request,
        Protos::AcquireLockResp * response,
        google::protobuf::Closure * done) override;

    void releaseLock(
        google::protobuf::RpcController * cntl,
        const Protos::ReleaseLockReq * request,
        Protos::ReleaseLockResp * response,
        google::protobuf::Closure * done) override;

    void assertLockAcquired(
        google::protobuf::RpcController * cntl,
        const Protos::AssertLockReq * request,
        Protos::AssertLockResp * response,
        google::protobuf::Closure * done) override;

    void reportCnchLockHeartBeat(
        google::protobuf::RpcController * cntl,
        const Protos::ReportCnchLockHeartBeatReq * request,
        Protos::ReportCnchLockHeartBeatResp * response,
        google::protobuf::Closure * done) override;

    void getServerStartTime(
        google::protobuf::RpcController * cntl,
        const Protos::GetServerStartTimeReq * request,
        Protos::GetServerStartTimeResp * response,
        google::protobuf::Closure * done) override;

    /***
     *  About GlobalGC
     */
    void scheduleGlobalGC(
        google::protobuf::RpcController * cntl,
        const Protos::ScheduleGlobalGCReq * request,
        Protos::ScheduleGlobalGCResp * response,
        google::protobuf::Closure * done) override;

    void getNumOfTablesCanSendForGlobalGC(
        google::protobuf::RpcController * cntl,
        const Protos::GetNumOfTablesCanSendForGlobalGCReq * request,
        Protos::GetNumOfTablesCanSendForGlobalGCResp * response,
        google::protobuf::Closure * done) override;

    void getDeletingTablesInGlobalGC(
        google::protobuf::RpcController * cntl,
        const Protos::GetDeletingTablesInGlobalGCReq * request,
        Protos::GetDeletingTablesInGlobalGCResp * response,
        google::protobuf::Closure * done) override;

    // About Auto Statistics
    void queryUdiCounter(
        [[maybe_unused]] google::protobuf::RpcController* controller,
        const Protos::QueryUdiCounterReq* request,
        Protos::QueryUdiCounterResp* response,
        google::protobuf::Closure* done) override;

    void redirectUdiCounter(
        google::protobuf::RpcController* controller,
        const Protos::RedirectUdiCounterReq* request,
        Protos::RedirectUdiCounterResp* response,
        google::protobuf::Closure* done) override;

    void scheduleDistributeUdiCount(
        google::protobuf::RpcController* controller,
        const Protos::ScheduleDistributeUdiCountReq* request,
        Protos::ScheduleDistributeUdiCountResp* response,
        google::protobuf::Closure* done) override;

    void scheduleAutoStatsCollect(
        google::protobuf::RpcController* controller,
        const Protos::ScheduleAutoStatsCollectReq* request,
        Protos::ScheduleAutoStatsCollectResp* response,
        google::protobuf::Closure* done) override;

    void redirectAsyncStatsTasks(
        google::protobuf::RpcController* controller,
        const Protos::RedirectAsyncStatsTasksReq* request,
        Protos::RedirectAsyncStatsTasksResp* response,
        google::protobuf::Closure* done) override;

    // forward part commit request to host server.
    void handleRedirectCommitRequest(
        google::protobuf::RpcController * controller,
        const Protos::RedirectCommitPartsReq * request,
        Protos::RedirectCommitPartsResp * response,
        google::protobuf::Closure * done,
        bool final_commit);

    void redirectCommitParts(
        google::protobuf::RpcController * controller,
        const Protos::RedirectCommitPartsReq * request,
        Protos::RedirectCommitPartsResp * response,
        google::protobuf::Closure * done) override;

    void redirectClearParts(
        google::protobuf::RpcController * controller,
        const Protos::RedirectClearPartsReq * request,
        Protos::RedirectClearPartsResp * response,
        google::protobuf::Closure * done) override;

    void redirectSetCommitTime(
        google::protobuf::RpcController * controller,
        const Protos::RedirectCommitPartsReq * request,
        Protos::RedirectCommitPartsResp * response,
        google::protobuf::Closure * done) override;

    void redirectAttachDetachedS3Parts(
        google::protobuf::RpcController* controller,
        const Protos::RedirectAttachDetachedS3PartsReq * request,
        Protos::RedirectAttachDetachedS3PartsResp * response,
        google::protobuf::Closure * done) override;

    void redirectDetachAttachedS3Parts(
        google::protobuf::RpcController* controller,
        const Protos::RedirectDetachAttachedS3PartsReq * request,
        Protos::RedirectDetachAttachedS3PartsResp * response,
        google::protobuf::Closure * done) override;

    void removeMergeMutateTasksOnPartitions(
        google::protobuf::RpcController * cntl,
        const Protos::RemoveMergeMutateTasksOnPartitionsReq * request,
        Protos::RemoveMergeMutateTasksOnPartitionsResp * response,
        google::protobuf::Closure * done) override;

    void submitQueryWorkerMetrics(
        google::protobuf::RpcController * cntl,
        const Protos::SubmitQueryWorkerMetricsReq * request,
        Protos::SubmitQueryWorkerMetricsResp * response,
        google::protobuf::Closure * done) override;

    void submitPreloadTask(
        google::protobuf::RpcController * cntl,
        const Protos::SubmitPreloadTaskReq * request,
        Protos::SubmitPreloadTaskResp * response,
        google::protobuf::Closure * done) override;

    void executeOptimize(
        google::protobuf::RpcController * cntl,
        const Protos::ExecuteOptimizeQueryReq * request,
        Protos::ExecuteOptimizeQueryResp * response,
        google::protobuf::Closure * done) override;

    void submitBackupTask(
        google::protobuf::RpcController * cntl,
        const Protos::SubmitBackupTaskReq * request,
        Protos::SubmitBackupTaskResp * response,
        google::protobuf::Closure * done) override;

    void getRunningBackupTask(
        google::protobuf::RpcController * cntl,
        const Protos::GetRunningBackupTaskReq * request,
        Protos::GetRunningBackupTaskResp * response,
        google::protobuf::Closure * done) override;

    void removeRunningBackupTask(
        google::protobuf::RpcController * cntl,
        const Protos::RemoveRunningBackupTaskReq * request,
        Protos::RemoveRunningBackupTaskResp * response,
        google::protobuf::Closure * done) override;

    void notifyAccessEntityChange(
        google::protobuf::RpcController *,
        const Protos::notifyAccessEntityChangeReq * request,
        Protos::notifyAccessEntityChangeResp * response,
        google::protobuf::Closure *done) override;

    void handleRefreshTaskOnFinish(
        google::protobuf::RpcController *,
        const Protos::handleRefreshTaskOnFinishReq * request,
        Protos::handleRefreshTaskOnFinishResp * response,
        google::protobuf::Closure *done) override;

#if USE_MYSQL
    void submitMaterializedMySQLDDLQuery(
        google::protobuf::RpcController * cntl,
        const Protos::SubmitMaterializedMySQLDDLQueryReq * request,
        Protos::SubmitMaterializedMySQLDDLQueryResp * response,
        google::protobuf::Closure * done) override;

    void reportHeartbeatForSyncThread(
        google::protobuf::RpcController * cntl,
        const Protos::ReportHeartbeatForSyncThreadReq * request,
        Protos::ReportHeartbeatForSyncThreadResp * response,
        google::protobuf::Closure * done) override;

    void reportSyncFailedForSyncThread(
        google::protobuf::RpcController * cntl,
        const Protos::ReportSyncFailedForSyncThreadReq * request,
        Protos::ReportSyncFailedForSyncThreadResp * response,
        google::protobuf::Closure * done) override;
#endif

    void forceRecalculateMetrics(
        google::protobuf::RpcController * cntl,
        const Protos::ForceRecalculateMetricsReq * request,
        Protos::ForceRecalculateMetricsResp * response,
        google::protobuf::Closure * done) override;

    void getLastModificationTimeHints(
        google::protobuf::RpcController * cntl,
        const Protos::getLastModificationTimeHintsReq * request,
        Protos::getLastModificationTimeHintsResp * response,
        google::protobuf::Closure * done) override;

    void notifyTableCreated(
        google::protobuf::RpcController * cntl,
        const Protos::notifyTableCreatedReq * request,
        Protos::notifyTableCreatedResp * response,
        google::protobuf::Closure * done) override;

    void getDedupImplVersion(
        google::protobuf::RpcController * cntl,
        const Protos::GetDedupImplVersionReq * request,
        Protos::GetDedupImplVersionResp * response,
        google::protobuf::Closure * done) override;

    void checkDelayInsertOrThrowIfNeeded(
        google::protobuf::RpcController * cntl,
        const Protos::checkDelayInsertOrThrowIfNeededReq * request,
        Protos::checkDelayInsertOrThrowIfNeededResp * response,
        google::protobuf::Closure * done) override;

private:
    const UInt64 server_start_time;
    std::optional<GlobalGCManager> global_gc_manager;
    LoggerPtr log;
};

REGISTER_SERVICE_IMPL(CnchServerServiceImpl);

}
