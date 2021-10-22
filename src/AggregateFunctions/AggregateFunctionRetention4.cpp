#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRetention4.h>
#include <AggregateFunctions/Helpers.h>
#include <common/LocalDate.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Int8) return new AggregateFunctionTemplate<Int8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int16) return new AggregateFunctionTemplate<Int16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int32) return new AggregateFunctionTemplate<Int32>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int64) return new AggregateFunctionTemplate<Int64>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int128) return new AggregateFunctionTemplate<Int128>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt128) return new AggregateFunctionTemplate<UInt128>(std::forward<TArgs>(args)...);
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionRetention4(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * array_first = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
    const auto * array_retention =  checkAndGetDataType<DataTypeArray>(argument_types[1].get());
    if (!array_first || !array_retention)
        throw Exception("Arguments for function " + name + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    size_t params_size = parameters.size();

    if (params_size != 3)
        throw Exception("This instantiation of " + name + "aggregate function doesn't accept " + toString(params_size) + " parameters, should be 3",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    UInt64 ret_window = parameters[0].safeGet<UInt64>();
    DayNum start_date = LocalDate(parameters[1].safeGet<String>()).getDayNum();
    DayNum end_date = LocalDate(parameters[2].safeGet<String>()).getDayNum();

    if(start_date > end_date)
        throw Exception("The start_date should be less than end_date", ErrorCodes::LOGICAL_ERROR);

    AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionRetention4>(*array_first[0].getNestedType(), ret_window, start_date, end_date, argument_types, parameters));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

}

void registerAggregateFunctionRetention4(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retention4", createAggregateFunctionRetention4, AggregateFunctionFactory::CaseInsensitive);
}

}
