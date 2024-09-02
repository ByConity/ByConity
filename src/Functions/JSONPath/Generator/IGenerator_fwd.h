#pragma once

#include <memory>
#include <Functions/JSONPath/Generator/IVisitor.h>
#include <Functions/JSONPath/Generator/IObjectJSONVisitor.h>

namespace DB
{
template <typename JSONParser>
class IGenerator;

template <typename JSONParser>
using IVisitorPtr = std::shared_ptr<IVisitor<JSONParser>>;

template <typename JSONParser>
using VisitorList = std::vector<IVisitorPtr<JSONParser>>;

class IObjectJSONGenerator;
using IObjectJSONVisitorPtr = std::shared_ptr<IObjectJSONVisitor>;
using ObjectJSONVisitorList = std::vector<IObjectJSONVisitorPtr>;

}
