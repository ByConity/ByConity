#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};

using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveUTF8>, NamePositionUTF8>;

}

REGISTER_FUNCTION(PositionUTF8)
{
    factory.registerFunction<FunctionPositionUTF8>();
}

}
