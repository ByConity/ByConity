#pragma once
#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <common/types.h>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Block.h>

namespace DB
{
class IndexColumnPath
{

public:
    // column ptr and column offset ptr
    using RecursiveColumnBuildCallback = std::function<void(const ColumnPtr &, const std::vector<const ColumnPtr> &)>;
    IndexColumnPath(const String & column_name, const std::vector<String> & paths);
    // for unit test and log
    String printNodeTree() const;
    // column_with_type is must a tuple type
    // std::vector<const ColumnPtr &> is a offset stack to get elem nums to read
    void buildIndexFromTuple(const ColumnWithTypeAndName & column_with_type, RecursiveColumnBuildCallback callback) const;
private:
    class Node
    {
    public:
        bool need_index = false;
        String node_name;
        std::unordered_map<String, size_t> child_map;
        std::vector<size_t> child_indexes;
        explicit Node(const String & node_name_) : node_name(node_name_){}
    };
    void buildNode(const String & column_name, const std::vector<String> & paths);
    void buildNodeFromPaths(const std::vector<String> & paths);
    void buildIndexFromTupleOrSubcloumnImpl(const ColumnWithTypeAndName & current_column_with_type, const Node & current_node, std::vector<const ColumnPtr> & offsets_stack, RecursiveColumnBuildCallback callback) const;
    void buildIndexFromTupleAllSubcloumnImpl(const ColumnWithTypeAndName & current_column_with_type, std::vector<const ColumnPtr> & offsets_stack, RecursiveColumnBuildCallback callback) const;
    static inline bool isNestedElemnt(const DataTypePtr & data_type);
    void printNodeTreeImpl(std::stringstream & sstream, const std::string & prefix, size_t node_index) const;
    std::vector<Node> path_nodes;
};

using IndexColumnPathPtr = std::shared_ptr<IndexColumnPath>;
}
