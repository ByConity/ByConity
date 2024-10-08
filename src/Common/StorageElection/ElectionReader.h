/*
 * Copyright (2023) Bytedance Ltd. and/or its affiliates
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
#include <boost/noncopyable.hpp>
#include <Common/HostWithPorts.h>
#include <common/logger_useful.h>
#include <Common/StorageElection/KvStorage.h>
#include <Protos/cnch_common.pb.h>

#include <mutex>
#include <optional>

namespace DB
{

class ElectionReader : public boost::noncopyable
{
public:
    ElectionReader(IKvStoragePtr store_, const String & election_key_);

    bool refresh();
    HostWithPorts getLeaderInfo();
    std::optional<HostWithPorts> tryGetLeaderInfo();

private:
    std::optional<HostWithPorts> curr_leader_info{std::nullopt};
    mutable std::mutex leader_info_mutex;
    IKvStoragePtr store;
    String election_key;
    LoggerPtr logger = nullptr;
};

}
