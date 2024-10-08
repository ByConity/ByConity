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

#include <DaemonManager/TargetServerCalculator.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Catalog/Catalog.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

namespace DaemonManager
{

TargetServerCalculator::TargetServerCalculator(Context & context_, CnchBGThreadType type_, LoggerPtr log_)
    : type{type_}, context(context_), log{log_}
{}

CnchServerClientPtr TargetServerCalculator::getTargetServer(const StorageID & storage_id, UInt64 ts) const
{
    if (type == CnchBGThreadType::Consumer)
        return getTargetServerForCnchKafka(storage_id, ts);

    return getTargetServerForCnchMergeTree(storage_id, ts);
}

CnchServerClientPtr TargetServerCalculator::getTargetServerForCnchMergeTree(const StorageID & storage_id, UInt64 ts) const
{
    ts = (ts == 0) ? context.getTimestamp() : ts;
    auto target_server = context.getCnchTopologyMaster()->getTargetServer(toString(storage_id.uuid), storage_id.server_vw_name, ts, false);
    return context.getCnchServerClientPool().get(target_server);
}

CnchServerClientPtr TargetServerCalculator::getTargetServerForCnchKafka(const StorageID & storage_id, UInt64) const
{
    auto catalog = context.getCnchCatalog();
    /// Consume manager should be on the same server as the target table
    auto kafka_storage = catalog->tryGetTableByUUID(context, UUIDHelpers::UUIDToString(storage_id.uuid), TxnTimestamp::maxTS());
    if (!kafka_storage)
    {
        LOG_INFO(log, "Cannot get Kafka table by UUID for {}", storage_id.getNameForLogs());
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Cannot get Kafka table by UUID for {}", storage_id.getNameForLogs());
    }

    auto dependencies = catalog->getAllViewsOn(context, kafka_storage, TxnTimestamp::maxTS());
    if (dependencies.empty())
    {
        LOG_INFO(log, "No dependencies found for {}", storage_id.getNameForLogs());
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "No dependencies found for {}", storage_id.getNameForLogs());
    }

    /// For multiple MVs, we will try to use the first target table by default
    for (const auto & dependence : dependencies)
    {
        if (auto * mv_table = dynamic_cast<StorageMaterializedView*>(dependence.get()))
        {
            if (mv_table->async())
                continue;
            /// XXX: We cannot get target table from context here, we may store target table storageID in MV later
            if (auto cnch_table = catalog->tryGetTable(context, mv_table->getTargetDatabaseName(),
                                                         mv_table->getTargetTableName(), TxnTimestamp::maxTS()))
            {
                if (auto * cnch_storage = dynamic_cast<StorageCnchMergeTree*>(cnch_table.get()))
                {
                    auto target_server = context.getCnchTopologyMaster()->getTargetServer(toString(cnch_storage->getStorageUUID()),
                                                                                          cnch_storage->getServerVwName(), false);
                    return context.getCnchServerClientPool().get(target_server);
                }
                else
                {
                    LOG_ERROR(log, "Target table should be CnchMergeTree for " + storage_id.getNameForLogs());
                    throw Exception("Target table should be CnchMergeTree for " + storage_id.getNameForLogs(), ErrorCodes::BAD_REQUEST_PARAMETER);
                }
            }
            else
            {
                LOG_ERROR(log, "Target table not found for MV " + mv_table->getStorageID().getFullTableName());
                throw Exception("Target table not found for MV " + mv_table->getStorageID().getFullTableName(), ErrorCodes::BAD_REQUEST_PARAMETER);
            }
        }
        else
        {
            LOG_WARNING(log, "Expected MaterializedView but got " + dependence->getName() + " for " + dependence->getTableName());
        }
    }

    return nullptr;
}

} /// end namespace

}
