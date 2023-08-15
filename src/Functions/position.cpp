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

struct NameLocate
{
    static constexpr auto name = "locate";
};

using FunctionPosition = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NamePosition>;

using FunctionLocate = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NameLocate>;
}

void registerFunctionPosition(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPosition>(FunctionFactory::CaseInsensitive);
}

void registerFunctionLocate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLocate>(FunctionFactory::CaseInsensitive);
}
}
