#pragma once

#include <vector>
#include <Protos/cnch_common.pb.h>

namespace DB
{
using Checkpoint = Protos::Checkpoint;
using Checkpoints = std::vector<Checkpoint>;
}
