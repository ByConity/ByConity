#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFinderGroupFunnel.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
namespace
{
    AggregateFunctionPtr createAggregateFunctionFinderGroupFunnel(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        // The fuction signature is vvfunnelOpt(_,_,_,_,UIDX)(T, CT, U, _, ....)
        if (params.size() != 5 && params.size() != 6  && params.size() != 7 && params.size() != 8 && params.size() != 9)
            throw Exception("Aggregate function " + name + " requires (windows_in_seconds, start_time, check_granularity, number_check, [related_num], [relative_window_type, time_zone], [time_interval]), "
                                + "whose optional parameters will be used for attribute association and current day window", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (argument_types.size() < 4)
            throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be at least four.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!argument_types[0]->equals(*argument_types[1]))
            throw Exception("First two columns should be the same type for aggregate function " + name,
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeUInt64 *>(argument_types[0].get()))
            throw Exception("First two columns should be the same type as UInt64 " + name,
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 attr_related = 0;
        if (params.size() == 6 || params.size() == 8 || params.size() == 9)
            attr_related = params[5].safeGet<UInt64>();

        size_t v = attr_related ?  __builtin_popcount(attr_related)+3 : 3;
        bool event_check = v < argument_types.size();
        for (size_t i = v; i < argument_types.size(); i++)
        {
            if (!typeid_cast<const DataTypeUInt8 *>(argument_types[i].get()))
            {
                event_check = false;
                break;
            }
        }
        if (!event_check)
            throw Exception("Illegal types for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        UInt64 window = params[0].safeGet<UInt64>();
        UInt64 watch_start = params[1].safeGet<UInt64>();
        UInt64 watch_step = params[2].safeGet<UInt64>();
        UInt64 watch_numbers = params[3].safeGet<UInt64>();
        UInt64 user_pro_idx = params[4].safeGet<UInt64>();

        UInt64 window_type = 0;
        String time_zone;
        if (params.size() == 7)
        {
            window_type = params[5].safeGet<UInt64>();
            // wind_type = 0: not a relative procedure (default)
            // wind_type = 1: relative window of the same day of the first event to be checked
            if (!(window_type == 0 || window_type == 1))
                throw Exception("Illegal window_type value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            time_zone = params[6].safeGet<String>();
        }

        if (params.size() == 8 || params.size() == 9)
        {
            window_type = params[6].safeGet<UInt64>();
            if (!(window_type == 0 || window_type == 1))
                throw Exception("Illegal window_type value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            time_zone = params[7].safeGet<String>();
        }

        bool time_interval = false;
        if (params.size() == 9)
            time_interval = params[8].safeGet<UInt64>() > 0;

        UInt64 num_virts = argument_types.size() - v;
        // Limitation right now, we only support up to 64 events.
        if (num_virts > NUMBER_STEPS)
            throw Exception("Too many events checked in " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (user_pro_idx > num_virts || user_pro_idx < 1)
            throw Exception("Wrong input for user property index, 1-based.",ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr res = nullptr;
        DataTypePtr user_pro_type = argument_types[2];
        if (user_pro_type->isNullable())
            user_pro_type = static_cast<const DataTypeNullable *>(user_pro_type.get())->getNestedType();

        DataTypePtr attr_type = std::make_shared<DataTypeUInt8>();
        if (attr_related)
        {
            attr_type = argument_types[3];
            for (int i = 1; i < __builtin_popcount(attr_related); i++)
            {
                if (argument_types[i+3]->getTypeId() != attr_type->getTypeId())
                    throw Exception("Inconsistent types of associated attributes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        if (typeid_cast<const DataTypeString*>(user_pro_type.get()) ||
            typeid_cast<const DataTypeFixedString*>(user_pro_type.get()))
        {
            res.reset(createWithSingleTypeLastKnown<AggregateFunctionFinderGroupFunnel>(
                *attr_type, window, watch_start, watch_step,
                watch_numbers, user_pro_idx, window_type, time_zone, num_virts, attr_related, time_interval, argument_types, params));
        }
        else
        {
            res.reset(createWithTypesAndIntegerType<AggregateFunctionFinderGroupNumFunnel>(
                *user_pro_type, *attr_type, window, watch_start, watch_step,
                watch_numbers, user_pro_idx, window_type, time_zone, num_virts, attr_related, time_interval, argument_types, params));
        }

        if (!res)
            throw Exception("Illegal types for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionFinderGroupFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("finderGroupFunnel", createAggregateFunctionFinderGroupFunnel, AggregateFunctionFactory::CaseInsensitive);
}
}
