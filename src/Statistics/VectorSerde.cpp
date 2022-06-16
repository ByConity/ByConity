#include <DataTypes/IDataType.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/SerdeUtils.h>
#include <Statistics/TypeMacros.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
template <typename T>
std::string vector_serialize(const std::vector<T> & data)
{
    // TODO: optimize it to use better encoding
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T> || IsWideInteger<T>);
    static_assert(std::is_trivial_v<T>);
    // bool not supported due to stupid vector<bool>
    static_assert(!std::is_same_v<bool, T>);
    const char * ptr = reinterpret_cast<const char *>(data.data());
    auto bytes = sizeof(T) * data.size();
    return std::string(ptr, bytes);
}

template <typename T>
std::vector<T> vector_deserialize(std::string_view blob)
{
    // TODO: optimize it to use better encoding
    // TODO: support string
    if (blob.size() % sizeof(T) != 0)
    {
        throw Exception("Corrupted Blob", ErrorCodes::LOGICAL_ERROR);
    }
    auto size = blob.size() / sizeof(T);
    std::vector<T> vec(size);
    memcpy(vec.data(), blob.data(), blob.size());
    return vec;
}

#define INITIALIZE(TYPE) template std::string vector_serialize<TYPE>(const std::vector<TYPE> & data);
FIXED_TYPE_ITERATE(INITIALIZE)
#undef INITIALIZE

#define INITIALIZE(TYPE) template std::vector<TYPE> vector_deserialize<TYPE>(std::string_view blob);
FIXED_TYPE_ITERATE(INITIALIZE)
#undef INITIALIZE
}
