#pragma once
#include <memory>
#include <vector>

namespace DB
{
class DataModelPartWrapper;
class ServerDataPart;

using DataModelPartWrapperPtr = std::shared_ptr<DataModelPartWrapper>;
using DataModelPartWrapperVector = std::vector<DataModelPartWrapperPtr>;
using ServerDataPartPtr = std::shared_ptr<const ServerDataPart>;
using ServerDataPartsVector = std::vector<ServerDataPartPtr>;
}
