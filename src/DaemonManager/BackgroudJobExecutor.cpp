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

#include <DaemonManager/BackgroudJobExecutor.h>
#include <DaemonManager/BackgroundJob.h>
#include <CloudServices/CnchServerClient.h>

namespace DB::DaemonManager
{

namespace
{

void executeServerBGThreadAction(const StorageID & storage_id, const String & host_port, CnchBGThreadAction action, 
    const Context & context, CnchBGThreadType type, const std::optional<UInt64>& timeout_ms)
{
    CnchServerClientPtr server_client = context.getCnchServerClient(host_port);
    server_client->controlCnchBGThread(storage_id, type, action, timeout_ms);
    LOG_DEBUG(getLogger(__func__), "Succeed to {} thread for {} on {}",
        toString(action), storage_id.getNameForLogs(), host_port);
}

} /// end anonymous namespace

bool IBackgroundJobExecutor::start(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms)
{
    return start(info.storage_id, info.host_port, timeout_ms);
}

bool IBackgroundJobExecutor::stop(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms)
{
    return stop(info.storage_id, info.host_port, timeout_ms);
}

bool IBackgroundJobExecutor::remove(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms)
{
    return remove(info.storage_id, info.host_port, timeout_ms);
}

bool IBackgroundJobExecutor::drop(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms)
{
    return drop(info.storage_id, info.host_port, timeout_ms);
}

bool IBackgroundJobExecutor::wakeup(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms)
{
    return wakeup(info.storage_id, info.host_port, timeout_ms);
}

BackgroundJobExecutor::BackgroundJobExecutor(const Context & context_, CnchBGThreadType type_)
    : context{context_}, type{type_}
{}

bool BackgroundJobExecutor::start(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms)
{
    executeServerBGThreadAction(storage_id, host_port, CnchBGThreadAction::Start, context, type, timeout_ms);
    return true;
}

bool BackgroundJobExecutor::stop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms)
{
    executeServerBGThreadAction(storage_id, host_port, CnchBGThreadAction::Stop, context, type, timeout_ms);
    return true;
}

bool BackgroundJobExecutor::remove(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms)
{
    executeServerBGThreadAction(storage_id, host_port, CnchBGThreadAction::Remove, context, type, timeout_ms);
    return true;
}

bool BackgroundJobExecutor::drop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms)
{
    executeServerBGThreadAction(storage_id, host_port, CnchBGThreadAction::Drop, context, type, timeout_ms);
    return true;
}

bool BackgroundJobExecutor::wakeup(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms)
{
    executeServerBGThreadAction(storage_id, host_port, CnchBGThreadAction::Wakeup, context, type, timeout_ms);
    return true;
}

} // end namespace DB::DaemonManager
