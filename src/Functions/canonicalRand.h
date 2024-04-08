#include <common/types.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
struct NameCanonicalRand
{
    static constexpr auto name = "randCanonical";
};

struct CanonicalRandImpl
{
    static void execute(char * output, size_t size);
private:
    const static constexpr Float64 min = 0;
    const static constexpr Float64 max = 1;
};

class FunctionCanonicalRand : public FunctionRandomImpl<CanonicalRandImpl, Float64, NameCanonicalRand>
{
public:
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionCanonicalRand>(); }
};
}
