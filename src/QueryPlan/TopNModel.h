#pragma once
#include <Protos/EnumMacros.h>
#include <Protos/enum.pb.h>

namespace DB
{

ENUM_WITH_PROTO_CONVERTER(
    TopNModel, // enum name
    Protos::TopNModel, // proto enum message
    (ROW_NUMBER, 0),
    (RANKER, 1),
    (DENSE_RANK, 2));
}
