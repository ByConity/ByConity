#pragma once
#include <Core/Types.h>
#include <Protos/optimizer_statistics.pb.h>
namespace DB::Statistics
{
constexpr auto PROTO_VERSION = Protos::DbStats_Version_V2;
}
