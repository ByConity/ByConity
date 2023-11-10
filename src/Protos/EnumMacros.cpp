#include <Protos/EnumMacros.h>
#include <boost/preprocessor.hpp>
#include <fmt/format.h>
#include <Common/Exception.h>

namespace DB
{
void throwBetterEnumException(const char * type, const char * enum_name, int code)
{
    throw DB::Exception(fmt::format("Invalid {} value {} for type {}: ", type, code, enum_name), DB::ErrorCodes::PROTOBUF_BAD_CAST);
}
}
