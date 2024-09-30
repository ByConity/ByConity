#include <cstddef>
#include <functional>
#include <optional>
#include <sstream>
#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexInvertedHelper.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}
IndexColumnPath::IndexColumnPath(const String & column_name, const std::vector<String> & paths)
{
    buildNode(column_name, paths);
}

void IndexColumnPath::buildNode(const String & column_name, const std::vector<String> & paths)
{
    path_nodes.clear();
    Node top_node(column_name);
    path_nodes.emplace_back(top_node);
    buildNodeFromPaths(paths);
}

void IndexColumnPath::buildNodeFromPaths(const std::vector<String> & paths)
{
    if (paths.empty())
    {
        throw Exception("indexed path of JSON/Tuple Type ls '*' or 'key' in data", ErrorCodes::BAD_ARGUMENTS);
    }
    if (paths[0] == "*")
    {
        path_nodes[0].need_index = true;
        return;
    }
    for (auto const & path : paths)
    {
        std::vector<String> splited_paths;
        boost::split(splited_paths, path, boost::is_any_of("."));
        size_t current_node_idx = 0;
        for (auto const & splited_path : splited_paths)
        {
            auto it = path_nodes[current_node_idx].child_map.find(splited_path);
            if (it != path_nodes[current_node_idx].child_map.end())
            {
                current_node_idx = it->second;
            }
            else
            {
                size_t new_node_idx = path_nodes.size();
                Node node(splited_path);
                path_nodes.emplace_back(node);
                path_nodes[current_node_idx].child_indexes.push_back(new_node_idx);
                path_nodes[current_node_idx].child_map.insert({splited_path, new_node_idx});
                current_node_idx = new_node_idx;
            }
        }
        path_nodes[current_node_idx].need_index = true;
    }
}

// The array(JSON) / Nested(JSON) column in memory
bool IndexColumnPath::isNestedElemnt(const DataTypePtr & data_type)
{
    if (const auto * array_type = dynamic_cast<const DataTypeArray *>(data_type.get()); array_type)
    {
        return isTuple(array_type->getNestedType());
    }
    return false;
}

void IndexColumnPath::buildIndexFromTuple(const ColumnWithTypeAndName & column_with_type, RecursiveColumnBuildCallback callback) const
{
    if (!isTuple(column_with_type.type))
    {
        throw Exception("buildIndexFromTuple only support tuple", ErrorCodes::ILLEGAL_COLUMN);
    }
    std::vector<const ColumnPtr> offsets_stack;
    buildIndexFromTupleOrSubcloumnImpl(column_with_type, path_nodes[0], offsets_stack, callback);
}

void IndexColumnPath::buildIndexFromTupleOrSubcloumnImpl(
    const ColumnWithTypeAndName & current_column_with_type,
    const Node & current_node,
    std::vector<const ColumnPtr> & offsets_stack,
    RecursiveColumnBuildCallback callback) const
{
    if (isTuple(current_column_with_type.type))
    {
        const auto & column_tuple = assert_cast<const ColumnTuple &>(*current_column_with_type.column);
        const auto & column_type_tuple = assert_cast<const DataTypeTuple &>(*current_column_with_type.type);
        if (current_node.need_index)
        {
            buildIndexFromTupleAllSubcloumnImpl(current_column_with_type, offsets_stack, callback);
        }
        else
        {
            for (const auto & child_index : current_node.child_indexes)
            {
                auto child_node_name = path_nodes[child_index].node_name;
                auto index_in_tuple = column_type_tuple.tryGetPositionByName(child_node_name);
                if (index_in_tuple)
                {
                    ColumnWithTypeAndName subcolumn_with_type(
                        column_tuple.getColumnPtr(index_in_tuple.value()),
                        column_type_tuple.getElement(index_in_tuple.value()),
                        column_type_tuple.getElementNames()[index_in_tuple.value()]);
                    buildIndexFromTupleOrSubcloumnImpl(subcolumn_with_type, path_nodes[child_index], offsets_stack, callback);
                }
            }
        }
    }
    else if (isNestedElemnt(current_column_with_type.type))
    {
        const auto & array_tuple_column = assert_cast<const ColumnArray &>(*current_column_with_type.column);
        const auto & array_tuple_column_type = assert_cast<const DataTypeArray &>(*current_column_with_type.type);
        auto tuple_column = array_tuple_column.getDataPtr();
        auto tuple_column_type = array_tuple_column_type.getNestedType();
        // nested(name) == current_node.node_name
        ColumnWithTypeAndName nested_column_type_and_name(tuple_column, tuple_column_type, current_node.node_name);
        offsets_stack.push_back(array_tuple_column.getOffsetsPtr());
        buildIndexFromTupleOrSubcloumnImpl(nested_column_type_and_name, current_node, offsets_stack, callback);
        offsets_stack.pop_back();
    }
    else if (isStringOrFixedString(current_column_with_type.type))
    {
        callback(current_column_with_type.column, offsets_stack);
    }
    else if (isArrayOfString(current_column_with_type.type))
    {
        const auto & array_string_column = assert_cast<const ColumnArray &>(*current_column_with_type.column);
        offsets_stack.push_back(array_string_column.getOffsetsPtr());
        callback(array_string_column.getDataPtr(), offsets_stack);
        offsets_stack.pop_back();
    }
    else
    {
        // if type not fit, do noting
    }
}

void IndexColumnPath::buildIndexFromTupleAllSubcloumnImpl(
    const ColumnWithTypeAndName & current_column_with_type,
    std::vector<const ColumnPtr> & offsets_stack,
    RecursiveColumnBuildCallback callback) const
{
    const auto & column_type_tuple = assert_cast<const DataTypeTuple &>(*current_column_with_type.type);
    const auto & column_tuple = assert_cast<const ColumnTuple &>(*current_column_with_type.column);
    const auto & elements = column_type_tuple.getElements();
    const auto & element_names = column_type_tuple.getElementNames();
    for (size_t i = 0; i < elements.size(); i++)
    {
        if (isStringOrFixedString(elements[i]))
        {
            callback(column_tuple.getColumnPtr(i), offsets_stack);
        }
        else if (isArrayOfString(elements[i]))
        {
            const auto & array_string_column = assert_cast<const ColumnArray &>(*column_tuple.getColumnPtr(i));
            offsets_stack.push_back(array_string_column.getOffsetsPtr());
            callback(array_string_column.getDataPtr(), offsets_stack);
            offsets_stack.pop_back();
        }
        else if (isTuple(elements[i]))
        {
            ColumnWithTypeAndName subcolumn_with_type(column_tuple.getColumnPtr(i), elements[i], element_names[i]);
            buildIndexFromTupleAllSubcloumnImpl(subcolumn_with_type, offsets_stack, callback);
        }
        else if (isNestedElemnt(elements[i]))
        {
            const auto & array_tuple_column = assert_cast<const ColumnArray &>(*column_tuple.getColumnPtr(i));
            const auto & array_tuple_column_type = assert_cast<const DataTypeArray &>(*elements[i]);
            auto sub_tuple_column = array_tuple_column.getDataPtr();
            auto sub_tuple_column_type = array_tuple_column_type.getNestedType();
            // name is not used
            ColumnWithTypeAndName nested_column_type_and_name(sub_tuple_column, sub_tuple_column_type, "");
            offsets_stack.push_back(array_tuple_column.getOffsetsPtr());
            buildIndexFromTupleAllSubcloumnImpl(nested_column_type_and_name, offsets_stack, callback);
            offsets_stack.pop_back();
        }
    }
}

String IndexColumnPath::printNodeTree() const
{
    std::stringstream sstream;
    printNodeTreeImpl(sstream, "", 0);
    return sstream.str();
}

void IndexColumnPath::printNodeTreeImpl(std::stringstream & sstream, const std::string & prefix, size_t node_index) const
{
    sstream << prefix << path_nodes[node_index].node_name;
    if (path_nodes[node_index].need_index)
        sstream << "(indexed)";
    sstream << "\n";
    for (const auto & index : path_nodes[node_index].child_indexes)
    {
        printNodeTreeImpl(sstream, prefix + "|--", index);
    }
}

}
