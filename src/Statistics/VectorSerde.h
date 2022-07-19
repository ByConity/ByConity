#pragma once
#include <vector>
namespace DB::Statistics
{
template <typename T>
std::vector<T> vector_deserialize(std::string_view blob);

template <typename T>
std::string vector_serialize(const std::vector<T> & data);
}
