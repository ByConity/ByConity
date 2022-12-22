#pragma once
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/TypeMacros.h>

namespace DB::Statistics
{
using SerdeDataType = Protos::SerdeDataType;
static_assert(sizeof(SerdeDataType) == 4);

template <typename T>
inline constexpr SerdeDataType SerdeDataTypeFrom = SerdeDataType::Nothing;

#define CASE(TYPE) \
    template <> \
    inline constexpr SerdeDataType SerdeDataTypeFrom<TYPE> = SerdeDataType::TYPE;
ALL_TYPE_ITERATE(CASE)
#undef CASE


} // namespace DB
