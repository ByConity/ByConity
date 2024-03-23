#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionOrderBy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "OrderBy"; }

    DataTypes transformArguments(const DataTypes & arguments, const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(
                "Incorrect number of parameters for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt32 num_params = parameters.back().safeGet<UInt32>();

        if (arguments.size() < num_params)
            throw Exception(
                "Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return DataTypes(arguments.begin(), std::prev(arguments.end(), num_params));
    }

    Array transformParameters(const Array & parameters) const override
    {
        if (parameters.empty())
            throw Exception(
                "Incorrect number of parameters for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt32 num_params = parameters.back().safeGet<UInt32>();

        if (parameters.size() < num_params)
            throw Exception(
                "Incorrect number of parameters for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return Array(parameters.begin(), std::prev(parameters.end(), num_params + 1));
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        UInt32 num_params = params.back().safeGet<UInt32>();
        std::vector<bool> is_ascending(num_params);

        for (size_t i = 1; i <= num_params; ++i)
            is_ascending[i - 1] = static_cast<bool>(params.rbegin()[i].safeGet<UInt8>());

        return std::make_shared<AggregateFunctionOrderBy>(nested_function, std::move(is_ascending), arguments, params);
    }
};

void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
}

}
