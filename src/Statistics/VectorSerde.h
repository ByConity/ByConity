#pragma once
#include <vector>
namespace DB::Statistics
{
template <typename T>
std::vector<T> vectorDeserialize(std::string_view blob);

template <typename T>
std::string vectorSerialize(const std::vector<T> & data);
}
