#pragma once

#include <memory>
#include <vector>
#include <roaring.hh>

namespace DB
{
class HiveDataPart;

using HiveDataPartCNCHPtr = std::shared_ptr<const HiveDataPart>;
using HiveDataPartsCNCHVector = std::vector<HiveDataPartCNCHPtr>;
using MutableHiveDataPartCNCHPtr = std::shared_ptr<HiveDataPart>;
using MutableHiveDataPartsCNCHVector = std::vector<MutableHiveDataPartCNCHPtr>;
}
