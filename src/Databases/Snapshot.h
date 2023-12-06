#pragma once

#include <Protos/data_models.pb.h>
#include <common/types.h>
#include <fmt/format.h>
#include <memory>
#include <vector>

namespace DB
{

using SnapshotPtr = std::shared_ptr<Protos::DataModelSnapshot>;
using Snapshots = std::vector<SnapshotPtr>;

inline String toString(const Protos::DataModelSnapshot & snapshot)
{
    return fmt::format("{{ name={}, ts={}, ttl={} }}", snapshot.name(), snapshot.commit_time(), snapshot.ttl_in_days());
}

};
