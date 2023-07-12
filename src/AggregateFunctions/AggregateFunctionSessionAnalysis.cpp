/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSessionAnalysis.h>

#include <Common/typeid_cast.h>
#include <common/constexpr_helpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

/*
* sessionAnalysis(max_seesion_size,start_event,end_event,target_event)(event, time,param...)
* vSessionAnalysis(max_seesion_size,start_event,end_event,target_event)(event,time,flag,param...)
*/
template<bool with_virtual_event=false>
AggregateFunctionPtr createAggregateFunctionSessionAnalysis(const String & name, const DataTypes & argument_types, const Array & params, const Settings * )
{
    if (params.size() != 4 && params.size() != 5)
    {
        throw Exception("Aggregate function " + name + " requires 4/5 parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    size_t need_argument_size = 2 + (with_virtual_event ? 1 : 0);
    if (argument_types.size() < need_argument_size)
    {
        throw Exception(
            "Aggregate function " + name + " requires not less than " + std::to_string(need_argument_size) + " arguments, but parsed "
                + std::to_string(argument_types.size()),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    std::vector<TypeIndex> need_argument_types;
    if constexpr(with_virtual_event)
        need_argument_types = {TypeIndex::String, TypeIndex::UInt64, TypeIndex::UInt8};
    else
        need_argument_types = {TypeIndex::String, TypeIndex::UInt64};

    for (size_t i = 0; i < need_argument_types.size(); ++i)
    {
        auto which = WhichDataType(argument_types[i]);

        if (which.idx != need_argument_types[i])
        {
            throw Exception("Aggregate function " + name + " " + std::to_string(i + 1) + " col should not be " + argument_types[i]->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    UInt64 max_seesion_size = params[0].safeGet<UInt64>();
    String start_event = params[1].safeGet<String>();
    String end_event = params[2].safeGet<String>();
    String target_event = params[3].safeGet<String>();

    if (max_seesion_size == 0)
        throw Exception("Aggregate function " + name + " session size should be non-zero.", ErrorCodes::BAD_ARGUMENTS);

    bool has_start_event = !start_event.empty();
    bool has_end_event = !end_event.empty();
    bool has_target_event = !target_event.empty();

    AggregateFunctionPtr res;
    static_for<0, 2 * 2 * 2>([&](auto ijklm) {
        constexpr bool i = ijklm & (1 << 0);
        constexpr bool j = ijklm & (1 << 1);
        constexpr bool k = ijklm & (1 << 2);
        if (i == has_start_event && j == has_end_event && k == has_target_event)
        {
            res = std::make_shared<AggregateFunctionSessionAnalysis<i, j, k, with_virtual_event>>(
                max_seesion_size, start_event, end_event, target_event, argument_types, params);
            return true;
        }
        return false;
    });
    return res;
}

void registerAggregateFunctionSessionAnalysis(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sessionAnalysis", createAggregateFunctionSessionAnalysis<false>, AggregateFunctionFactory::CaseSensitive);
    factory.registerFunction("vSessionAnalysis", createAggregateFunctionSessionAnalysis<true>, AggregateFunctionFactory::CaseSensitive);
}

}
