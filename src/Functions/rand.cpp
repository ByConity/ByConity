#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/canonicalRand.h>

namespace DB
{
namespace
{

struct NameRand { static constexpr auto name = "rand"; };
struct FunctionRand : public FunctionRandom<UInt32, NameRand>
{
    explicit FunctionRand(ContextPtr context)
        : FunctionRandom(context)
    {}

    static FunctionPtr create(ContextPtr context)
    {
        if (context->getSettings().dialect_type == DialectType::MYSQL)
            return FunctionCanonicalRand::create(context);
        else
            return std::make_shared<FunctionRand>(context);
    }
};

}

REGISTER_FUNCTION(Rand)
{
    factory.registerFunction<FunctionRand>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("rand32", NameRand::name);
}

}

