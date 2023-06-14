#include <Functions/FunctionsStringSearch.h>
#include <Functions/PositionImpl.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace {
struct NameInstr
{
    static constexpr auto name = "instr";
};

using FunctionInstr = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NameInstr>;

}

void registerFunctionInstr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionInstr>(FunctionFactory::CaseInsensitive);
}

}

