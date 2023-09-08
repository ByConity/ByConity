#pragma once
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
    Poco::Logger * logger = nullptr;
};

}
