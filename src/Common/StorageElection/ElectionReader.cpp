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

#include <Common/StorageElection/ElectionReader.h>
#include <Common/HostWithPorts.h>
#include <Protos/RPCHelpers.h>
#include <Protos/cnch_common.pb.h>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_A_LEADER;
}

ElectionReader::ElectionReader(IKvStoragePtr store_, const String & election_key_)
    : store(std::move(store_)), election_key(election_key_), logger(getLogger("ElectionReader"))
{
    LOG_INFO(logger, "election_key: {}", election_key);
}

HostWithPorts ElectionReader::getLeaderInfo()
{
    std::lock_guard lock(leader_info_mutex);
    if (curr_leader_info)
        return curr_leader_info.value();

    throw Exception(ErrorCodes::NOT_A_LEADER, "Can't get current leader from {}", election_key);
}

std::optional<HostWithPorts> ElectionReader::tryGetLeaderInfo()
{
    std::lock_guard lock(leader_info_mutex);
    return curr_leader_info;
}

bool ElectionReader::refresh()
{
    try
    {
        String result;
        auto version = store->get(election_key, result);

        if (version == 0)
        {
            LOG_ERROR(logger, "Visit election_key {} failed, version: {}", election_key, version);
            return false;
        }
        if (result.empty())
        {
            LOG_ERROR(logger, "Key {} is not created, maybe the leader is not elected ", election_key);
            return false;
        }

        Protos::LeaderInfo latest_leader_info;
        if (!latest_leader_info.ParseFromString(result))
        {
            LOG_ERROR(logger, "Unknown format for election storage!");
            return false;
        }

        if (latest_leader_info.lease().status() != Protos::LeaderLease::Ready)
            return false;

        std::optional<HostWithPorts> prev_leader;
        auto host_info = RPCHelpers::createHostWithPorts(latest_leader_info.address());
        {
            std::lock_guard lock(leader_info_mutex);
            prev_leader = std::move(curr_leader_info);
            curr_leader_info = std::move(host_info);
        }

        LOG_INFO(logger, "Updated leader from {} to {}", prev_leader ? prev_leader->toDebugString() : "Empty", curr_leader_info->toDebugString());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

}
