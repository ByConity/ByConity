#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>


namespace DB
{
namespace
{
    struct CotName
    {
        static constexpr auto name = "cot";
    };

    Float64 cot(Float64 r)
    {
        Float64 cot = 1.0 / tan(r);
        return cot;
    }

    using FunctionCot = FunctionMathUnary<UnaryFunctionVectorized<CotName, cot>>;
}

REGISTER_FUNCTION(Cot)
{
    factory.registerFunction<FunctionCot>(FunctionFactory::CaseInsensitive);
}

}
