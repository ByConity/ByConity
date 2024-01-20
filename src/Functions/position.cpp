#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePosition
{
    static constexpr auto name = "position";
};

using FunctionPosition = FunctionsStringSearch<PositionImpl<NamePosition, PositionCaseSensitiveASCII>>;

struct NameLocate
{
    static constexpr auto name = "locate";
};

using FunctionLocate = FunctionsStringSearch<PositionImpl<NameLocate, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(Position)
{
    factory.registerFunction<FunctionPosition>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLocate>(FunctionFactory::CaseInsensitive);
}
}
