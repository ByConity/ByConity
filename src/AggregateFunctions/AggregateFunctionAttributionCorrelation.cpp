#include "AggregateFunctionAttributionCorrelation.h"
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{
namespace
{
    AggregateFunctionPtr createAggregateFunctionAttributionCorrelation(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        auto throw_invalid_arguments_exception = [&name]() {
            throw Exception("Aggregate function " + name + " need Array(Tuple(Tuple(UInt16, Array), UInt64, UInt64, Array, Array, Array)) arguments", ErrorCodes::BAD_ARGUMENTS);
        };

        bool valid_arguments = false;
        DataTypePtr attr_type = std::make_shared<DataTypeUInt64>();

        if (argument_types.empty())
            throw_invalid_arguments_exception();
        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get()))
        {
            if (const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(array_type->getNestedType().get()))
            {
                const DataTypes& element_types = tuple_type->getElements();
                if (element_types.size() < 6)
                    throw_invalid_arguments_exception();
                if (const auto * touch_event_tuple = checkAndGetDataType<DataTypeTuple>(element_types[0].get()))
                {
                    if (touch_event_tuple->getElements().size() < 2)
                        throw_invalid_arguments_exception();
                    if (const auto * attr_array_type = checkAndGetDataType<DataTypeArray>(touch_event_tuple->getElements()[1].get()))
                    {
                        attr_type = attr_array_type->getNestedType();
                        valid_arguments = true;
                    }
                }
            }
        }
        
        if (!valid_arguments)
            throw_invalid_arguments_exception();

        if (params.size() > 2)
            throw Exception("Aggregate function " + name + " need no more than two params", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 N = 0;
        if(!params.empty())
            N = params[0].safeGet<UInt64>();

        bool need_others = false;
        if (params.size() == 2)
            need_others = params[1].safeGet<UInt64>() > 0;

        return AggregateFunctionPtr(createWithTimeAttrTypeSingle<AggregateFunctionAttributionCorrelation>(*attr_type, N, need_others, argument_types, params));
    }
}

void registerAggregateFunctionAttributionCorrelation(AggregateFunctionFactory & factory)
{
    factory.registerFunction("attributionCorrelation", createAggregateFunctionAttributionCorrelation,
                             AggregateFunctionFactory::CaseInsensitive);
}
}
