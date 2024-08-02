#include <AggregateFunctions/AggregateFunctionThetaSketchEstimate.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitors.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeSketchBinary.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

    template <UInt8 K>
    struct WithK
    {
        template <typename T>
        using AggregateFunction = AggregateFunctionThetaSketchEstimate<T, K>;
    };

    template <UInt8 K>
    AggregateFunctionPtr createAggregateFunctionWithK(const DataTypes & argument_types, const Array & params)
    {
        const IDataType & argument_type = *argument_types[0];
        WhichDataType which(argument_type);

        bool ignore_wrong_date = argument_types.size() == 2;

        if (which.isSketchBinary())
        {
           return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeSketchBinary>>(argument_types, params, ignore_wrong_date);
        }
        else if (which.isAggregateFunction())
        {
           return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeAggregateFunction>>(argument_types, params, ignore_wrong_date);
        }
        else if (which.isString())
        {
           return std::make_shared<typename WithK<K>::template AggregateFunction<String>>(argument_types, params, ignore_wrong_date);
        }
        else
        {
           throw Exception("Incorrect columns type for aggregate function: " + argument_type.getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }


    AggregateFunctionPtr createAggregateFunctionThetaSketchEstimate
        (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        UInt8 precision = 15;
        if (!params.empty())
        {
            if (params.size() != 1)
            {
                throw Exception(
                    "Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
            // This range is hardcoded below
            if (precision_param > 26 || precision_param < 5)
            {
                throw Exception(
                    "Parameter for aggregate function " + name + "is out or range: [5, 26].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }

            precision = precision_param;
        }
        if (argument_types.size() != 1 && argument_types.size() != 2)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        switch (precision)
        {
            case 5:
                return createAggregateFunctionWithK<5>(argument_types, params);
            case 6:
                return createAggregateFunctionWithK<6>(argument_types, params);
            case 7:
                return createAggregateFunctionWithK<7>(argument_types, params);
            case 8:
                return createAggregateFunctionWithK<8>(argument_types, params);
            case 9:
                return createAggregateFunctionWithK<9>(argument_types, params);
            case 10:
                return createAggregateFunctionWithK<10>(argument_types, params);
            case 11:
                return createAggregateFunctionWithK<11>(argument_types, params);
            case 12:
                return createAggregateFunctionWithK<12>(argument_types, params);
            case 13:
                return createAggregateFunctionWithK<13>(argument_types, params);
            case 14:
                return createAggregateFunctionWithK<14>(argument_types, params);
            case 15:
                return createAggregateFunctionWithK<15>(argument_types, params);
            case 16:
                return createAggregateFunctionWithK<16>(argument_types, params);
            case 17:
                return createAggregateFunctionWithK<17>(argument_types, params);
            case 18:
                return createAggregateFunctionWithK<18>(argument_types, params);
            case 19:
                return createAggregateFunctionWithK<19>(argument_types, params);
            case 20:
                return createAggregateFunctionWithK<20>(argument_types, params);
            case 21:
                return createAggregateFunctionWithK<21>(argument_types, params);
            case 22:
                return createAggregateFunctionWithK<22>(argument_types, params);
            case 23:
                return createAggregateFunctionWithK<23>(argument_types, params);
            case 24:
                return createAggregateFunctionWithK<24>(argument_types, params);
            case 25:
                return createAggregateFunctionWithK<25>(argument_types, params);
            case 26:
                return createAggregateFunctionWithK<26>(argument_types, params);
        }

        __builtin_unreachable();
    }
}

void registerAggregateFunctionThetaSketchEstimate(AggregateFunctionFactory & factory)
{
    factory.registerFunction("thetaSketchEstimate", createAggregateFunctionThetaSketchEstimate);
}
}
