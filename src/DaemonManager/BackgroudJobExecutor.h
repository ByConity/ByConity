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

#include <Interpreters/Context_fwd.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <Interpreters/StorageID.h>

namespace DB::DaemonManager
{

struct BGJobInfo;
class IBackgroundJobExecutor
{
public:
    bool start(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms = std::nullopt);
    bool stop(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms = std::nullopt);
    bool remove(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms = std::nullopt);
    bool drop(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms = std::nullopt);
    bool wakeup(const BGJobInfo & info, const std::optional<UInt64>& timeout_ms = std::nullopt);

    virtual bool start(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) = 0;
    virtual bool stop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) = 0;
    virtual bool remove(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) = 0;
    virtual bool drop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) = 0;
    virtual bool wakeup(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) = 0;

    virtual ~IBackgroundJobExecutor() = default;
};

class BackgroundJobExecutor : public IBackgroundJobExecutor
{
public:
    BackgroundJobExecutor(const Context & context, CnchBGThreadType type);
    BackgroundJobExecutor() = delete;
    BackgroundJobExecutor(const BackgroundJobExecutor &) = delete;
    BackgroundJobExecutor(BackgroundJobExecutor &&) = delete;
    BackgroundJobExecutor & operator = (const BackgroundJobExecutor &) = delete;
    BackgroundJobExecutor & operator = (BackgroundJobExecutor &&) = delete;
    bool start(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) override;
    bool stop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) override;
    bool remove(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) override;
    bool drop(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) override;
    bool wakeup(const StorageID & storage_id, const String & host_port, const std::optional<UInt64>& timeout_ms) override;
private:
    const Context & context;
    const CnchBGThreadType type;
};

}
