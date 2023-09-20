/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Interpreters/WindowDescription.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeHelper.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorsAccurateComparison.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string WindowFunctionDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window function '" << column_name << "\n";
    ss << "function node " << (function_node != nullptr ? function_node->dumpTree() : "") << "\n";
    ss << "aggregate function '" << aggregate_function->getName() << "'\n";
    if (!function_parameters.empty())
    {
        ss << "parameters " << toString(function_parameters) << "\n";
    }

    return ss.str();
}

std::string WindowDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window '" << window_name << "'\n";
    ss << "partition_by " << dumpSortDescription(partition_by) << "\n";
    ss << "order_by " << dumpSortDescription(order_by) << "\n";
    ss << "full_sort_description " << dumpSortDescription(full_sort_description) << "\n";

    return ss.str();
}

std::string WindowFrame::toString() const
{
    WriteBufferFromOwnString buf;
    toString(buf);
    return buf.str();
}

void WindowFrame::toString(WriteBuffer & buf) const
{
    buf << toString(type) << " BETWEEN ";
    if (begin_type == BoundaryType::Current)
    {
        buf << "CURRENT ROW";
    }
    else if (begin_type == BoundaryType::Unbounded)
    {
        buf << "UNBOUNDED";
        buf << " "
            << (begin_preceding ? "PRECEDING" : "FOLLOWING");
    }
    else
    {
        buf << applyVisitor(FieldVisitorToString(), begin_offset);
        buf << " "
            << (begin_preceding ? "PRECEDING" : "FOLLOWING");
    }
    buf << " AND ";
    if (end_type == BoundaryType::Current)
    {
        buf << "CURRENT ROW";
    }
    else if (end_type == BoundaryType::Unbounded)
    {
        buf << "UNBOUNDED";
        buf << " "
            << (end_preceding ? "PRECEDING" : "FOLLOWING");
    }
    else
    {
        buf << applyVisitor(FieldVisitorToString(), end_offset);
        buf << " "
            << (end_preceding ? "PRECEDING" : "FOLLOWING");
    }
}

void WindowFrame::checkValid() const
{
    // Check the validity of offsets.
    if (type == WindowFrame::FrameType::Rows
        || type == WindowFrame::FrameType::Groups)
    {
        if (begin_type == BoundaryType::Offset
            && !((begin_offset.getType() == Field::Types::UInt64
                    || begin_offset.getType() == Field::Types::Int64)
                && begin_offset.get<Int64>() >= 0
                && begin_offset.get<Int64>() < INT_MAX))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Frame start offset for '{}' frame must be a nonnegative 32-bit integer, '{}' of type '{}' given",
                toString(type),
                applyVisitor(FieldVisitorToString(), begin_offset),
                Field::Types::toString(begin_offset.getType()));
        }

        if (end_type == BoundaryType::Offset
            && !((end_offset.getType() == Field::Types::UInt64
                    || end_offset.getType() == Field::Types::Int64)
                && end_offset.get<Int64>() >= 0
                && end_offset.get<Int64>() < INT_MAX))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Frame end offset for '{}' frame must be a nonnegative 32-bit integer, '{}' of type '{}' given",
                toString(type),
                applyVisitor(FieldVisitorToString(), end_offset),
                Field::Types::toString(end_offset.getType()));
        }
    }

    // Check relative positioning of offsets.
    // UNBOUNDED PRECEDING end and UNBOUNDED FOLLOWING start should have been
    // forbidden at the parsing level.
    assert(!(begin_type == BoundaryType::Unbounded && !begin_preceding));
    assert(!(end_type == BoundaryType::Unbounded && end_preceding));

    if (begin_type == BoundaryType::Unbounded
        || end_type == BoundaryType::Unbounded)
    {
        return;
    }

    if (begin_type == BoundaryType::Current
        && end_type == BoundaryType::Offset
        && !end_preceding)
    {
        return;
    }

    if (end_type == BoundaryType::Current
        && begin_type == BoundaryType::Offset
        && begin_preceding)
    {
        return;
    }

    if (end_type == BoundaryType::Current
        && begin_type == BoundaryType::Current)
    {
        // BETWEEN CURRENT ROW AND CURRENT ROW makes some sense for RANGE or
        // GROUP frames, and is technically valid for ROWS frame.
        return;
    }

    if (end_type == BoundaryType::Offset
        && begin_type == BoundaryType::Offset)
    {
        // Frame start offset must be less or equal that the frame end offset.
        bool begin_less_equal_end;
        if (begin_preceding && end_preceding)
        {
            /// we can't compare Fields using operator<= if fields have different types
            begin_less_equal_end = applyVisitor(FieldVisitorAccurateLessOrEqual(), end_offset, begin_offset);
        }
        else if (begin_preceding && !end_preceding)
        {
            begin_less_equal_end = true;
        }
        else if (!begin_preceding && end_preceding)
        {
            begin_less_equal_end = false;
        }
        else /* if (!begin_preceding && !end_preceding) */
        {
            begin_less_equal_end = applyVisitor(FieldVisitorAccurateLessOrEqual(), begin_offset, end_offset);
        }

        if (!begin_less_equal_end)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Frame start offset {} {} does not precede the frame end offset {} {}",
                begin_offset, begin_preceding ? "PRECEDING" : "FOLLOWING",
                end_offset, end_preceding ? "PRECEDING" : "FOLLOWING");
        }
        return;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Window frame '{}' is invalid",
        toString());
}

void WindowDescription::checkValid() const
{
    frame.checkValid();

    // RANGE OFFSET requires exactly one ORDER BY column.
    if (frame.type == WindowFrame::FrameType::Range
        && (frame.begin_type == WindowFrame::BoundaryType::Offset
            || frame.end_type == WindowFrame::BoundaryType::Offset)
        && order_by.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The RANGE OFFSET window frame requires exactly one ORDER BY column, {} given",
           order_by.size());
    }
}

void WindowFrame::toProto(Protos::WindowFrame & proto) const
{
    proto.set_is_default(is_default);
    proto.set_type(WindowFrame::FrameTypeConverter::toProto(type));
    proto.set_begin_type(WindowFrame::BoundaryTypeConverter::toProto(begin_type));
    begin_offset.toProto(*proto.mutable_begin_offset());
    proto.set_begin_preceding(begin_preceding);
    proto.set_end_type(WindowFrame::BoundaryTypeConverter::toProto(end_type));
    end_offset.toProto(*proto.mutable_end_offset());
    proto.set_end_preceding(end_preceding);
}

void WindowFrame::fillFromProto(const Protos::WindowFrame & proto)
{
    is_default = proto.is_default();
    type = FrameTypeConverter::fromProto(proto.type());
    begin_type = BoundaryTypeConverter::fromProto(proto.begin_type());
    begin_offset.fillFromProto(proto.begin_offset());
    begin_preceding = proto.begin_preceding();
    end_type = BoundaryTypeConverter::fromProto(proto.end_type());
    end_offset.fillFromProto(proto.end_offset());
    end_preceding = proto.end_preceding();
}

void WindowFunctionDescription::toProto(Protos::WindowFunctionDescription & proto) const
{
    proto.set_column_name(column_name);
    serializeAggregateFunctionToProto(aggregate_function, function_parameters, argument_types, *proto.mutable_aggregate_function());

    for (const auto & element : argument_names)
        proto.add_argument_names(element);
}

void WindowFunctionDescription::fillFromProto(const Protos::WindowFunctionDescription & proto)
{
    column_name = proto.column_name();
    std::tie(aggregate_function, function_parameters, argument_types) = deserializeAggregateFunctionFromProto(proto.aggregate_function());

    for (const auto & element : proto.argument_names())
        argument_names.emplace_back(element);
}

void WindowDescription::toProto(Protos::WindowDescription & proto) const
{
    proto.set_window_name(window_name);
    for (const auto & element : partition_by)
        element.toProto(*proto.add_partition_by());
    for (const auto & element : order_by)
        element.toProto(*proto.add_order_by());
    for (const auto & element : full_sort_description)
        element.toProto(*proto.add_full_sort_description());
    frame.toProto(*proto.mutable_frame());
    for (const auto & element : window_functions)
        element.toProto(*proto.add_window_functions());
}

void WindowDescription::fillFromProto(const Protos::WindowDescription & proto)
{
    window_name = proto.window_name();
    for (const auto & proto_element : proto.partition_by())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        partition_by.emplace_back(std::move(element));
    }
    for (const auto & proto_element : proto.order_by())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        order_by.emplace_back(std::move(element));
    }
    for (const auto & proto_element : proto.full_sort_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        full_sort_description.emplace_back(std::move(element));
    }
    frame.fillFromProto(proto.frame());
    for (const auto & proto_element : proto.window_functions())
    {
        WindowFunctionDescription element;
        element.fillFromProto(proto_element);
        window_functions.emplace_back(std::move(element));
    }
}
}
